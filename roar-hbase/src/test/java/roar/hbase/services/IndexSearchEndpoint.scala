package roar.hbase.services

import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.{CoprocessorEnvironment, Coprocessor}
import org.apache.hadoop.hbase.coprocessor.CoprocessorService
import roar.protocol.generated.RoarProtos.{SearchResponse, SearchRequest, IndexSearchService}

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-05
  */
class IndexSearchEndpoint extends IndexSearchService
    with CoprocessorService
    with Coprocessor{
  override def query(controller: RpcController, request: SearchRequest, done: RpcCallback[SearchResponse]): Unit = {
  }
  override def getService: Service = this

  override def stop(env: CoprocessorEnvironment): Unit = {
  }
  override def start(env: CoprocessorEnvironment): Unit = {
  }
}
