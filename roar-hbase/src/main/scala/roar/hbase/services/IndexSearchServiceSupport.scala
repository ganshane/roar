package roar.hbase.services

import java.io.IOException

import com.google.protobuf.{ByteString, RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.coprocessor.CoprocessorService
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import roar.hbase.internal.RegionSearchSupport
import roar.protocol.generated.RoarProtos._
import stark.utils.services.LoggerSupport

/**
  * rpc service for index search
 *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-05
  */
trait IndexSearchServiceSupport extends CoprocessorService {
  this:RegionSearchSupport with LoggerSupport =>
  private val emptyResponse = SearchResponse.newBuilder().setCount(0).setTotal(0).setMaxScore(0).build
  private val emptyIdResponse = IdSearchResponse.getDefaultInstance
  private val emptyGroupResponse = GroupCountSearchResponse.getDefaultInstance

  private val service = new IndexSearchService {
    override def query(controller: RpcController, request: SearchRequest, done: RpcCallback[SearchResponse]): Unit = {
      var finalResponse = emptyResponse
      try {
        info("[{}] query {}",request.getTableName,request.getQ)
        val sortOpt = if (request.hasSort) Some(request.getSort) else None
        val responseOpt = search(request.getQ, sortOpt, request.getTopN)
        responseOpt match {
          case Some(response) =>
            finalResponse = response
          case None =>
            error("[{}] response is empty",request.getTableName)
            controller.setFailed("response not found,resource not supported?")
        }
      }catch {
        case ioe:IOException =>
          error("fail to execute query",ioe)
          ResponseConverter.setControllerException(controller,ioe)
        case other:Throwable=>
          error("fail to execute query",other)
          controller.setFailed(other.toString)
      }finally{
        done.run(finalResponse)
      }

    }

    override def idQuery(controller: RpcController, request: IdSearchRequest, done: RpcCallback[IdSearchResponse]): Unit ={
      var finalResponse = emptyIdResponse
      try {
        val responseOpt = searchObjectId(request.getQ,request.getObjectIdField)
        responseOpt match {
          case Some(response) =>
            finalResponse = response
          case None =>
            controller.setFailed("response not found,resource not supported?")
        }
      }catch {
        case ioe:IOException =>
          error("fail to execute query",ioe)
          ResponseConverter.setControllerException(controller,ioe)
        case other:Throwable=>
          error("fail to execute query",other)
          controller.setFailed(other.toString)
      }finally{
        done.run(finalResponse)
      }

    }

    override def freqQuery(controller: RpcController, request: GroupCountSearchRequest, done: RpcCallback[GroupCountSearchResponse]): Unit = {
      var finalResponse = emptyGroupResponse
      try {
        val responseOpt = searchFreq(request.getQ,request.getGroupField,request.getMaxGroup,request.getTopN)
        responseOpt match {
          case Some(data) =>
            val builder = GroupCountSearchResponse.newBuilder()
            data._1.foreach{g=>
              val rb = builder.addResultBuilder()
              rb.setName(ByteString.copyFromUtf8(g.name.utf8ToString()))
              rb.setCount(g.count)
            }
            builder.setPartialGroup(data._2)
            builder.setTotalDoc(numDoc)
            builder.setHitDoc(data._3)
            finalResponse = builder.build()
          case None =>
            controller.setFailed("response not found,resource not supported?")
        }
      }catch {
        case ioe:IOException =>
          error("fail to execute query",ioe)
          ResponseConverter.setControllerException(controller,ioe)
        case other:Throwable=>
          error("fail to execute query",other)
          controller.setFailed(other.toString)
      }finally{
        done.run(finalResponse)
      }
    }
  }
  override def getService: Service = service
}
