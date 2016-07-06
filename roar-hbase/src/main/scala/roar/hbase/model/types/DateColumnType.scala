// Copyright 2011,2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.model.types

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.Cell
import org.apache.lucene.document.{Field, IntField, NumericDocValuesField}
import roar.hbase.model.DataColumnType
import roar.hbase.model.ResourceDefinition.ResourceProperty
import roar.hbase.services.DataTypeUtils

/**
 * string column type
  *
  * @author jcai
 * @version 0.1
 */
object DateColumnType extends DateColumnType

class DateColumnType extends DataColumnType[Long] {
  private final val DEFAULT_DATE_FORMAT = "yyyyMMddHHmmss"
  override protected def convertCellAsData(cell: Cell): Option[Long] = {
    Some(ByteBuffer.wrap(cell.getValueArray,cell.getValueOffset,cell.getValueLength).getInt)
  }
  def convertDfsValueToString(value: Option[Long], cd: ResourceProperty): Option[String] = {
    if (value.isDefined) {
      val format = if (cd.apiFormat == null) DEFAULT_DATE_FORMAT else cd.apiFormat
      val formatter = new SimpleDateFormat(format)
      Some(formatter.format(new Date(value.get)))
    } else None
  }

  def createIndexField(value: Long, cd: ResourceProperty) = {
    val valueConverted = DataTypeUtils.convertDateAsInt(value)
    (new IntField(cd.name, valueConverted, IntField.TYPE_NOT_STORED),Some(new NumericDocValuesField(cd.name,value)))
  }

  def setIndexValue(f: (Field,Option[Field]), value: Long, cd: ResourceProperty) {
    f._1.setIntValue(DataTypeUtils.convertDateAsInt(value))
    f._2.foreach(_.setLongValue(value))
  }
}