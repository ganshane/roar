// Copyright 2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.services

import org.apache.hadoop.hbase.client.Result
import org.apache.lucene.document.Document
import roar.api.meta.{ObjectCategory, ResourceDefinition}
import roar.hbase.services.DocumentSource.ObjectIdSeqFinder

/**
 * 文档创建
  *
  * @author jcai
 */
trait DocumentCreator{
  def newDocument(rd:ResourceDefinition,result:Result,objectIdSeqFinder: ObjectIdSeqFinder): Document
}
object DocumentSource{
  type ObjectIdSeqFinder = (Array[Byte], ObjectCategory) => Int
}
trait DocumentSource{
  def newDocument(rd:ResourceDefinition,timestamp:Long,result:Result,objectIdSeqFinder: ObjectIdSeqFinder): Option[Document]
}

