package roar.hbase.services

import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.{CoprocessorEnvironment, Coprocessor}
import org.apache.hadoop.hbase.coprocessor.{RegionCoprocessorEnvironment, CoprocessorService}
import roar.protocol.generated.RoarProtos.{SearchResponse, SearchRequest, IndexSearchService}

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-05
  */
class IndexSearchEndpoint extends IndexSearchService
    with CoprocessorService
    with Coprocessor{
  private var regionId:Long =  -1
  override def query(controller: RpcController, request: SearchRequest, done: RpcCallback[SearchResponse]): Unit = {
    val docs = IndexSource.findIndex(regionId).search(request.getQ,request.getOffset,request.getLimit)
    val response = SearchResponse.newBuilder()
    response.setCount(docs.length)
    response.setTotal(100)

    done.run(response.build())
  }
  override def getService: Service = this

  override def stop(env: CoprocessorEnvironment): Unit = {
  }
  override def start(env: CoprocessorEnvironment): Unit = {
    regionId = env.asInstanceOf[RegionCoprocessorEnvironment].getRegionInfo.getRegionId
  }
}
