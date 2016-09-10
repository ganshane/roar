// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.internal

import java.util.concurrent.ExecutorService

import org.apache.lucene.index.{IndexReader, LeafReaderContext}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.util.BytesRef
import roar.api.meta.ResourceDefinition
import roar.hbase.RoarHbaseConstants

/**
 * 针对某一资源进行搜索,增加了id缓存的支持
  *
  * @author jcai
 */
class InternalIndexSearcher(reader: IndexReader, rd: ResourceDefinition, executor: ExecutorService)
  extends IndexSearcher(reader, executor)
  //with ObjectIdCacheSupport
  {
  /*
  //如果分析的话，加上数据的hash值
  if (findObjectIdColumn) {
    payloadLength = GlobalObjectIdCache.FULL_LENGTH
  }
  */

  /*
  for (readerContext <- leafContexts) {
    loadObjectIdWithLocalCache(rd.name, readerContext.reader().asInstanceOf[SegmentReader])
  }
  */

  def rowId(docId: Int): BytesRef= {
    val subReaderContext= getSubReaderContext(docId)
    val docValues = subReaderContext.reader().getBinaryDocValues(RoarHbaseConstants.ROW_ID_FIELD_NAME)
    docValues.get(docId-subReaderContext.docBaseInParent)
  }

  private def getSubReaderContext(n: Int): LeafReaderContext = {
    // find reader for doc n:
    val size = leafContexts.size()
    1 until size foreach { i =>
      val context = leafContexts.get(i)
      if (context.docBaseInParent > n) {
        return leafContexts.get(i - 1)
      }
    }
    leafContexts.get(size - 1)
  }
}

