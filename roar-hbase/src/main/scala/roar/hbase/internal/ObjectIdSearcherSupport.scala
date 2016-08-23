// Copyright 2015 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.internal

import javax.naming.SizeLimitExceededException

import com.google.protobuf.ByteString
import org.apache.lucene.index.{LeafReaderContext, MultiDocValues}
import org.apache.lucene.search.SimpleCollector
import roar.api.services.RoarSparseFixedBitSet
import roar.hbase.services.RegionCoprocessorEnvironmentSupport
import roar.protocol.generated.RoarProtos.IdSearchResponse
import stark.utils.services.LoggerSupport

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
  def searchObjectId(q: String): Option[IdSearchResponse]= {
    doInSearcher { search =>

      val parser = createParser()
      val query = parser.parse(q)
      logger.info("object id query :{} ....", q)
      val start = System.currentTimeMillis()
      val originCollector = new IdSearchCollector(search)
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

  private class IdSearchCollector(s: InternalIndexSearcher) extends SimpleCollector {
    //TODO 得到unique
    private val sortedSetDocValues = MultiDocValues.getSortedSetValues(s.getIndexReader,"object_id")
    private[internal] val result = new RoarSparseFixedBitSet(sortedSetDocValues.getValueCount.toInt)


//    var index: SortedSetDocValues = null
    var docBase:Int = 0

    override def doSetNextReader(context: LeafReaderContext): Unit = {
        //index = DocValues.getSortedSet(context.reader, "object_id")
//      logger.debug("next reader -> {}  valueCount:{} ",context.reader(),index.getValueCount)
      docBase = context.docBase
      logger.debug("next reader -> {}  docBase:{} ",context.reader(),docBase)
//      sortedSetDocValues.
    }


    def collect(doc: Int) {
      val idSeq = readObjectId(doc)
      logger.debug("doc:{} idseq:{}",doc,idSeq)
      if (idSeq < 0) return
      result.set(idSeq)
    }

    private def readObjectId(doc: Int): Int = {
      sortedSetDocValues.setDocument(doc+docBase)
//      val ord = sortedSetDocValues.nextOrd()
//      sortedSetDocValues.lookupOrd(ord)
      sortedSetDocValues.nextOrd().toInt
    }

    override def needsScores(): Boolean = false
  }
}
