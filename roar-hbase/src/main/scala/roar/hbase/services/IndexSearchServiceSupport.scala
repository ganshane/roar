package roar.hbase.services

import java.io.IOException

import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.coprocessor.CoprocessorService
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import roar.hbase.internal.RegionSearchSupport
import roar.protocol.generated.RoarProtos.{IndexSearchService, SearchRequest, SearchResponse}

/**
  * rpc service for index search
 *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-05
  */
trait IndexSearchServiceSupport extends CoprocessorService {
  this:RegionSearchSupport =>
  private val emptyResponse = SearchResponse.newBuilder().setCount(0).setTotal(0).setMaxScore(0).build

  private val service = new IndexSearchService {
    override def query(controller: RpcController, request: SearchRequest, done: RpcCallback[SearchResponse]): Unit = {
      var finalResponse = emptyResponse
      try {
        val sortOpt = if (request.hasSort) Some(request.getSort) else None
        val responseOpt = search(request.getQ, sortOpt, request.getTopN)
        responseOpt match {
          case Some(response) =>
            finalResponse = response
          case None =>
            controller.setFailed("response not found,resource not supported?")
        }
      }catch {
        case ioe:IOException =>
          ResponseConverter.setControllerException(controller,ioe)
        case other=>
          controller.setFailed(other.toString)
      }finally{
        done.run(finalResponse)
      }

    }
  }
  override def getService: Service = service
}
