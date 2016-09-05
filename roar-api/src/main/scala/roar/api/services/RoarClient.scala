package roar.api.services

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch.{Call, Callback}
import org.apache.hadoop.hbase.ipc.{BlockingRpcCallback, ServerRpcController}
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import org.apache.hadoop.io.IOUtils
import org.apache.lucene.search.{ScoreDoc, TopDocs}
import org.apache.lucene.util.PriorityQueue
import org.slf4j.LoggerFactory
import roar.protocol.generated.RoarProtos.SearchResponse.Row
import roar.protocol.generated.RoarProtos._

/**
  * roar client for hbase
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-09
  */
class RoarClient(conf:Configuration) {
  private val logger = LoggerFactory getLogger getClass
  //share one connection
  private lazy val connection = HConnectionManager.createConnection(conf)
  /**
    * call full text search to hbase cluster
    *
    * @param tableName table name searched
    * @param q query text
    * @param sortOpt sort option
    * @param offset offset in collection
    * @param size fetch size
    * @return search collection
    */
  def search(tableName:String, q: String,sortOpt:Option[String]=None,offset: Int=0, size: Int=30):SearchResponse ={
    internalSearch((table,searchRequest)=>{
      val list = new CopyOnWriteArrayList[SearchResponse]()
      table.coprocessorService(classOf[IndexSearchService], null, null,
        new Call[IndexSearchService, SearchResponse]() {
          override def call(instance: IndexSearchService): SearchResponse = {
            val controller: ServerRpcController = new ServerRpcController
            val rpcCallback = new BlockingRpcCallback[SearchResponse]
            instance.query(controller, searchRequest, rpcCallback)
            val response = rpcCallback.get
            if (controller.failedOnException) {
              throw controller.getFailedOn
            }
            response
          }
        }, new Callback[SearchResponse] {
          override def update(region: Array[Byte], row: Array[Byte], result: SearchResponse): Unit = {
            if(result != null)
              list.add(result)
          }
        })
      list.toArray(new Array[SearchResponse](list.size()))
    },tableName,q,sortOpt,offset,size)
  }
  def searchOnRS(tableName:String, q: String,sortOpt:Option[String]=None,offset: Int=0, size: Int=30):SearchResponse ={
    internalSearch((table,searchRequest)=>{
      val list = new CopyOnWriteArrayList[SearchResponse]()
      val response = SearchResponse.getDefaultInstance
      table.batchCoprocessorService(IndexSearchService.getDescriptor.findMethodByName("query"),
        searchRequest,null,null,response,new Callback[SearchResponse]() {
          override def update(region: Array[Byte], row: Array[Byte], result: SearchResponse): Unit = {
            if(result != null)
              list.add(result)
          }
        })
      list.toArray(new Array[SearchResponse](list.size()))
    },tableName,q,sortOpt,offset,size)
  }
  private def internalSearch(fun:(HTableInterface,SearchRequest)=>Array[SearchResponse],tableName:String, q: String,sortOpt:Option[String]=None,offset: Int=0, size: Int=30):SearchResponse ={
    doInTable(tableName) { table =>
      val searchRequestBuilder = SearchRequest.newBuilder()
      searchRequestBuilder.setTableName(tableName)
      searchRequestBuilder.setQ(q)
      sortOpt.foreach(searchRequestBuilder.setSort)
      searchRequestBuilder.setTopN(offset + size)

      val searchRequest = searchRequestBuilder.build()
      val result = fun(table,searchRequest)

      logger.info("list size:{}",result.length)
//      internalMerge(offset, size, result)
      mergeAux(offset,size,result)
    }
  }

  /**
    * id search  on hbase cluster
    *
    * @param tableName table name
    * @param objectIdField object id field name
    * @param q query
    * @return id search response
    */
  def idSearch(tableName:String, objectIdField:String,q: String):Array[IdSearchResponse] ={
    internalIdSearch((table,searchRequest)=>{
      val list = new CopyOnWriteArrayList[IdSearchResponse]()
      table.coprocessorService(classOf[IndexSearchService], null, null,
        new Call[IndexSearchService, IdSearchResponse]() {
          override def call(instance: IndexSearchService): IdSearchResponse = {
            val controller: ServerRpcController = new ServerRpcController
            val rpcCallback = new BlockingRpcCallback[IdSearchResponse]
            instance.idQuery(controller, searchRequest, rpcCallback)
            val response = rpcCallback.get
            val ioe = ResponseConverter.getControllerException(controller)
            if (ioe != null) {
              logger.error("exception on rpc:",ioe)
              throw ioe
            }
            response
          }
        }, new Callback[IdSearchResponse] {
          override def update(region: Array[Byte], row: Array[Byte], result: IdSearchResponse): Unit = {
            if(result != null)
              list.add(result)
          }
        })
      list.toArray(new Array[IdSearchResponse](list.size()))
    },tableName,objectIdField,q)
  }
  def idSearchOnRS(tableName:String, objectIdField:String,q: String):Array[IdSearchResponse]={
    internalIdSearch((table,searchRequest)=>{
      val list = new CopyOnWriteArrayList[IdSearchResponse]()
      val response = IdSearchResponse.getDefaultInstance
      table.batchCoprocessorService(IndexSearchService.getDescriptor.findMethodByName("idQuery"),
        searchRequest,null,null,response,new Callback[IdSearchResponse]() {
          override def update(region: Array[Byte], row: Array[Byte], result: IdSearchResponse): Unit = {
            if(result != null && result.hasRegionId)
              list.add(result)
          }
        })
      list.toArray(new Array[IdSearchResponse](list.size()))
    },tableName,objectIdField,q)
  }
  private def internalIdSearch(fun:(HTableInterface,IdSearchRequest)=>Array[IdSearchResponse],tableName:String,objectIdField:String,q: String):Array[IdSearchResponse] ={
    doInTable(tableName) { table =>
      val searchRequestBuilder = IdSearchRequest.newBuilder()
      searchRequestBuilder.setQ(q)
      searchRequestBuilder.setObjectIdField(objectIdField)

      val searchRequest = searchRequestBuilder.build()
      val result = fun(table,searchRequest)
      logger.info("id list size:{}",result.length)

      result
    }
  }

  /**
    * find Row Data from hbase
    *
    * @param tableName table name
    * @param rowId row id
    * @return result object
    */
  def findRow(tableName:String,rowId:Array[Byte]): Option[Result] ={
    doInTable(tableName){table=>
      val get = new Get(rowId)
      val result = table.get(get)
      if(result.isEmpty) None else Some(result)
    }
  }

  private def doInTable[T](tableName:String)(tableAction:(HTableInterface)=>T):T={
    try{
      val table = connection.getTable(tableName)
      try{
        //find table
        tableAction(table)
      }finally{
        IOUtils.closeStream(table)
      }
    }finally{
//     IOUtils.closeStream(connection)
    }
  }
  /*
  private def doInTable[T](tableName:String)(tableAction:(Table)=>T):T={
    var conn:Connection = null
    try {
      //get connection
      conn = ConnectionFactory.createConnection(conf)
      var table:Table = null
      try{
        //find table
        table = conn.getTable(TableName.valueOf(tableName))
        tableAction(table)
      }finally{
        table.close()
      }
    }finally{
      IOUtils.closeStream(conn)
    }
  }
  */
  private def internalMerge(offset:Int,size:Int,list:Array[SearchResponse]):SearchResponse={
    var shardIdx = 0
    var totalRecordNum = 0
    val docs = list.map{response=>
      val rows = response.getRowList
      val scoreDocs = Range(0,rows.size()).map{i=>
        new ScoreDoc(i,rows.get(i).getScore,shardIdx)
      }
      val docs = new TopDocs(response.getCount,scoreDocs.toArray,response.getMaxScore)

      shardIdx += 1
      totalRecordNum += response.getTotal

      docs
    }
    val result = TopDocs.merge(offset,size,docs.toArray)
    val searchResponseBuilder = SearchResponse.newBuilder()
    result.scoreDocs.foreach{scoreDoc=>
      val row = list(scoreDoc.shardIndex).getRow(scoreDoc.doc)
      searchResponseBuilder.addRow(row)
    }
    searchResponseBuilder.setCount(result.totalHits)
    searchResponseBuilder.setMaxScore(result.getMaxScore)
    searchResponseBuilder.setTotal(totalRecordNum)

    searchResponseBuilder.build()
  }
  private class ShardRef(val shardIndex:Int) {
    var hitIndex: Int = 0
    override def toString: String = {
      return "ShardRef(shardIndex=" + shardIndex + " hitIndex=" + hitIndex + ")"
    }
  }
  private class ScoreMergeSortQueue(shardHits:Array[SearchResponse]) extends PriorityQueue[ShardRef](shardHits.length) {

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

  private[services] def mergeAux(start: Int, size: Int, shardHits: Array[SearchResponse]):SearchResponse={
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
        val hit = shardHits(ref.shardIndex).getRow(ref.hitIndex)
        ref.hitIndex += 1

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
