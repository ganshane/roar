// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.internal

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.document.{Document, Field, _}
import org.apache.lucene.util.BytesRef
import roar.api.meta.ResourceDefinition
import roar.api.meta.ResourceDefinition.ResourceProperty
import roar.api.meta.ResourceDefinitionConversions._
import roar.hbase.RoarHbaseConstants
import roar.hbase.services.DocumentSource.ObjectIdSeqFinder
import roar.hbase.services.{DocumentCreator, DocumentSource}
import stark.utils.services.LoggerSupport

import scala.collection.JavaConversions._

/**
 * implements document source
  *
  * @author jcai
 */
class DocumentSourceImpl(factories: java.util.Map[String, DocumentCreator]) extends DocumentSource with LoggerSupport {
  //sid field
  private val sidField = new BinaryDocValuesField(RoarHbaseConstants.ROW_ID_FIELD_NAME, new BytesRef)
  private val cacheCreator = new ConcurrentHashMap[String, DocumentCreator]()
  private val rowIdField = new StringField(RoarHbaseConstants.ROW_ID_FIELD_NAME,"", Field.Store.NO)
  //TODO 调整为INT类型
  private val utField = new LongField(RoarHbaseConstants.UPDATE_TIME_FIELD_NAME, 1L, LongField.TYPE_NOT_STORED)


  override def newDocument(rd: ResourceDefinition, timestamp:Long,result: Result,objectIdSeqFinder: ObjectIdSeqFinder): Option[Document] = {

    //优先使用自定义的DocumentCreator
    var creator = factories.get(rd.name)

    //如果发现自定义为空，则进行创建默认DocumentCreator
    if (creator == null) {
      creator = cacheCreator.get(rd.name)
      if (creator == null) {
        var analyticsIdSeq:Option[Int] = None
        for ((col, index) <- rd.properties.view.zipWithIndex) {
          if (col.objectCategory != null) {
            if (analyticsIdSeq.isDefined) {
              warn("[{}] duplicate analytics id declaration", rd.name)
            }
            analyticsIdSeq = Some(index)
          }
        }
        val value = new DefaultDocumentCreator()
        creator = cacheCreator.putIfAbsent(rd.name, value)
        if (creator == null)
          creator = value
      }
    }
    val doc = creator.newDocument(rd,result,objectIdSeqFinder)
    if(doc.getFields.nonEmpty) {
      //用来快速更新
      rowIdField.setStringValue(Bytes.toString(result.getRow))
      doc.add(rowIdField)

      //用来获取对应的列值
      sidField.setBytesValue(result.getRow)
      doc.add(sidField)

      //设置更新时间
      utField.setLongValue(timestamp)
      doc.add(utField)

      Some(doc)
    } else {
      warn("[{}] no fields to indexed for key:{}",rd.name,Bytes.toString(result.getRow))
      None
    }
  }
}

class DefaultDocumentCreator extends DocumentCreator {
  private val cachedFields = scala.collection.mutable.Map[String, (Field,Option[Field])]()
  private val cachedSeqField = scala.collection.mutable.Map[String,Field]()

  override def newDocument(rd: ResourceDefinition, result: Result,objectIdSeqFinder: ObjectIdSeqFinder): Document = {
    val doc = new Document

    for ((col, index) <- rd.properties.view.zipWithIndex) {
      val valueOpt = col.readDfsValue(result)
      valueOpt match {
        case Some(value) =>
          val f = cachedFields.get(col.name)
          f match {
            case Some(field) =>
              setIndexValue(col, field, value)
              doc.add(field._1)
              //添加排序
              field._2.foreach(doc.add)


              //处理带有序列的
              cachedSeqField.get(col.name).foreach{idField=>
                val idSeqValue = col.readDfsValueAsByteArray(result).get
                val seq = objectIdSeqFinder.apply(idSeqValue,col.objectCategory)
                idField.setLongValue(seq)
                doc.add(idField)
              }
            case None =>
              val field = createFieldable(col, value)
              cachedFields.put(col.name, field)
              doc.add(field._1)
              //添加排序
              field._2.foreach(doc.add)

              if(col.objectCategory != null) {
                //添加ID序列字段
                val idField = col.createObjectIdField()
                cachedSeqField.put(col.name, idField)

                val idSeqValue = col.readDfsValueAsByteArray(result).get
                val seq = objectIdSeqFinder.apply(idSeqValue, col.objectCategory)
                idField.setLongValue(seq)
                doc.add(idField)
              }

          }
        case _ =>
        //
      }
    }

    doc
  }

  protected def createFieldable(col: ResourceProperty, value: Any) = {
    col.createIndexField(value)
  }

  protected def setIndexValue(col: ResourceProperty, f: (Field,Option[Field]), value: Any) {
    col.setIndexValue(f, value)
  }
}

