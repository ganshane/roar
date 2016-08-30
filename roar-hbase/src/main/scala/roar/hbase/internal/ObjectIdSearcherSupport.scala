// Copyright 2015 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.internal

import javax.naming.SizeLimitExceededException

import com.google.protobuf.ByteString
import org.apache.lucene.index.{LeafReaderContext, NumericDocValues}
import org.apache.lucene.search.SimpleCollector
import roar.api.services.RoarSparseFixedBitSet
import roar.hbase.services.{IndexHelper, RegionCoprocessorEnvironmentSupport}
import roar.protocol.generated.RoarProtos.IdSearchResponse
import stark.utils.services.LoggerSupport
import scala.collection.JavaConversions._

/**
  * 对象搜索的
 *
 * @author jcai
 */
trait ObjectIdSearcherSupport {
  this: SearcherManagerSupport
    with QueryParserSupport
    with RegionCoprocessorEnvironmentSupport
    with LoggerSupport =>
  /**
   * 搜索对象
 *
   * @param q 搜索条件
   * @return
   */
  def searchObjectId(q: String,field:String,maxSeq:Int=0): Option[IdSearchResponse]= {
    doInSearcher { search =>

      var idMaxSeq = maxSeq
      if(maxSeq ==0) {
        val columnOpt = queryResource.properties.find(_.name == field)
        val column = columnOpt.getOrElse(throw new RuntimeException("column definition not found by " + field))
        if(column.objectCategory == null)
          throw new RuntimeException("object category is null for "+field)
        idMaxSeq = IndexHelper.findCurrentSeq(region, column.objectCategory)
      }

      val parser = createParser()
      val query = parser.parse(q)
      logger.info("object id query :{} ....", q)
      val start = System.currentTimeMillis()
      val originCollector = new IdSearchCollector(search,field,idMaxSeq)
      try {
        /*
        var collector:Collector = new TimeOutCollector(originCollector)
        if(getIndexConfig.index.queryMaxLimit > 0)
          collector = new ResultLimitCollector(collector,getIndexConfig.index.queryMaxLimit)
          */
        search.search(query, originCollector)
      } catch {
        case e: SizeLimitExceededException =>
          logger.warn("over size limit")
      }
      //originCollector.result.optimize()
      val time = System.currentTimeMillis() - start
      val resultSize = originCollector.result.cardinality()
      logger.info("object id query :{},size:{} time:" + time, q, resultSize)

      val idShardResult = IdSearchResponse.newBuilder()
      val out = ByteString.newOutput(originCollector.result.ramBytesUsed().toInt)
      originCollector.result.serialize(out)
      idShardResult.setData(out.toByteString)
      idShardResult.setRegionId(region.getRegionId)

      idShardResult.build()
    }
  }

  private class IdSearchCollector(s: InternalIndexSearcher,field:String,maxSeq:Int) extends SimpleCollector {
    private[internal] val result = new RoarSparseFixedBitSet(maxSeq)
    private var idFieldValues:NumericDocValues = _

    override def doSetNextReader(context: LeafReaderContext): Unit = {
      idFieldValues = context.reader().getNumericDocValues(field)
    }


    def collect(doc: Int) {
      val idSeq = readObjectId(doc)
//      logger.debug("doc:{} idseq:{}",doc,idSeq)
      if (idSeq < 0) return
      result.set(idSeq)
    }

    private def readObjectId(doc: Int): Int = {
      idFieldValues.get(doc).toInt
    }

    override def needsScores(): Boolean = false
  }
}
