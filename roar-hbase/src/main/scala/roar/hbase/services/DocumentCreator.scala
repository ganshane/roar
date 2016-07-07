// Copyright 2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.services

import org.apache.hadoop.hbase.client.Result
import org.apache.lucene.document.Document
import roar.hbase.model.ResourceDefinition

/**
 * 文档创建
  *
  * @author jcai
 */
trait DocumentCreator{
  def newDocument(rd:ResourceDefinition,result:Result): Document
}
trait DocumentSource{
  def newDocument(rd:ResourceDefinition,timestamp:Long,result:Result): Option[Document]
}
