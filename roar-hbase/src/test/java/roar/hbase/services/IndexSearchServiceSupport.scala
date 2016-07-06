package roar.hbase.services

import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.coprocessor.CoprocessorService
import roar.protocol.generated.RoarProtos.{IndexSearchService, SearchRequest, SearchResponse}

/**
  * rpc service for index search
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-05
  */
trait IndexSearchServiceSupport extends CoprocessorService {
  this:RegionSearchSupport =>
  private val service = new IndexSearchService {
    override def query(controller: RpcController, request: SearchRequest, done: RpcCallback[SearchResponse]): Unit = {
      val docs = search(request.getQ, request.getOffset, request.getLimit)
      val response = SearchResponse.newBuilder()
      response.setCount(docs.length)
      response.setTotal(100)
      done.run(response.build())
    }
  }
  override def getService: Service = service
}
