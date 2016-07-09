package roar.api.services

import org.apache.hadoop.hbase.client.coprocessor.Batch.{Call, Callback}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.ipc.{BlockingRpcCallback, ServerRpcController}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.io.IOUtils
import org.apache.lucene.util.PriorityQueue
import roar.protocol.generated.RoarProtos.SearchResponse.Row
import roar.protocol.generated.RoarProtos.{IndexSearchService, SearchRequest, SearchResponse}

/**
  * roar client for hbase
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-09
  */
class RoarClient(conf:HBaseConfiguration) {


  def search(tableName:String, q: String,sortOpt:Option[String]=None,offset: Int=0, size: Int=30):SearchResponse ={
    var conn:Connection = null
    try {
      //get connection
      conn = ConnectionFactory.createConnection(conf)
      var table:Table = null
      try{
        //find table
        table = conn.getTable(TableName.valueOf(tableName))
        val searchRequestBuilder = SearchRequest.newBuilder()
        searchRequestBuilder.setTableName(tableName)
        searchRequestBuilder.setQ(q)
        sortOpt.foreach(searchRequestBuilder.setSort)
        searchRequestBuilder.setTopN(offset+size)

        val searchRequest = searchRequestBuilder.build()

        var list = List[SearchResponse]()
        table.coprocessorService(classOf[IndexSearchService],null,null,
          new Call[IndexSearchService,SearchResponse](){
            override def call(instance: IndexSearchService): SearchResponse = {
              val controller: ServerRpcController = new ServerRpcController
              val rpcCallback  = new BlockingRpcCallback[SearchResponse]
              instance.query(controller,searchRequest,rpcCallback)
              val response = rpcCallback.get
              if (controller.failedOnException) {
                throw controller.getFailedOn
              }
              response
            }
          },new Callback[SearchResponse] {
            override def update(region: Array[Byte], row: Array[Byte], result: SearchResponse): Unit = {
              list = result :: list
            }
          })
        mergeAux(offset,size,list)
      }finally{
        table.close()
      }
    }finally{
      IOUtils.closeStream(conn)
    }
  }

  private class ShardRef(val shardIndex:Int) {
    var hitIndex: Int = 0
    override def toString: String = {
      return "ShardRef(shardIndex=" + shardIndex + " hitIndex=" + hitIndex + ")"
    }
  }
  private class ScoreMergeSortQueue(shardHits:List[SearchResponse]) extends PriorityQueue[ShardRef](shardHits.length) {

    def lessThan(first: ShardRef, second: ShardRef): Boolean = {
      assert(first ne second)
      val firstScore: Float = shardHits(first.shardIndex).getRow(first.hitIndex).getScore
      val secondScore: Float = shardHits(second.shardIndex).getRow(second.hitIndex).getScore
      if (firstScore < secondScore) {
        return false
      }
      else if (firstScore > secondScore) {
        return true
      }
      else {
        if (first.shardIndex < second.shardIndex) {
          return true
        }
        else if (first.shardIndex > second.shardIndex) {
          return false
        }
        else {
          assert(first.hitIndex != second.hitIndex)
          return first.hitIndex < second.hitIndex
        }
      }
    }
  }

  private def mergeAux(start: Int, size: Int, shardHits: List[SearchResponse]):SearchResponse={
    val queue: PriorityQueue[ShardRef] = new ScoreMergeSortQueue(shardHits)
    var totalHitCount: Int = 0
    var totalRecordNum:Int = 0
    var availHitCount: Int = 0
    var maxScore: Float = Float.MinValue

    var shardIDX: Int = 0
    shardHits.foreach { shard =>
      totalHitCount += shard.getCount
      totalRecordNum += shard.getTotal

      if (shard.getRowCount > 0) {
        availHitCount += shard.getRowCount
        queue.add(new ShardRef(shardIDX))
        maxScore = Math.max(maxScore, shard.getMaxScore)
      }
      shardIDX += 1
    }

    if (availHitCount == 0) {
      maxScore = Float.NaN
    }
    var hits: Array[Row] = null
    if (availHitCount <= start) {
      hits = Array[Row]()
    }
    else {
      hits = new Array[Row](Math.min(size, availHitCount - start))

      val requestedResultWindow: Int = start + size
      val numIterOnHits: Int = Math.min(availHitCount, requestedResultWindow)
      var hitUpto: Int = 0
      while (hitUpto < numIterOnHits) {
        assert(queue.size > 0)
        val ref = queue.top
        ref.hitIndex += 1
        val hit = shardHits(ref.shardIndex).getRow(ref.hitIndex)

//        hit.shardIndex = ref.shardIndex

        if (hitUpto >= start) {
          hits(hitUpto - start) = hit
        }
        hitUpto += 1
        if (ref.hitIndex < shardHits(ref.shardIndex).getRowCount) {
          queue.updateTop
        }
        else {
          queue.pop
        }
      }
    }

    val searchResponseBuilder = SearchResponse.newBuilder()
    hits.foreach(searchResponseBuilder.addRow)
    searchResponseBuilder.setCount(totalHitCount)
    searchResponseBuilder.setMaxScore(maxScore)
    searchResponseBuilder.setTotal(totalRecordNum)

    searchResponseBuilder.build()
  }
}
