// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.model.types

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.Cell
import org.apache.lucene.document.{Field, LongField, NumericDocValuesField}
import roar.hbase.model.DataColumnType
import roar.hbase.model.ResourceDefinition.ResourceProperty

/**
 * Long Type Column
 *
 * @author jcai
 * @version 0.1
 */

object LongColumnType extends LongColumnType

class LongColumnType extends DataColumnType[Long] {
  override protected def convertCellAsData(cell: Cell): Option[Long] = {
    Some(ByteBuffer.wrap(cell.getValueArray,cell.getValueOffset,cell.getValueLength).getLong)
  }

  def convertDfsValueToString(value: Option[Long], cd: ResourceProperty) =
    if (value.isDefined) Some(value.get.toString) else None

  def createIndexField(value: Long, cd: ResourceProperty) =
    (new LongField(cd.name, value, LongField.TYPE_NOT_STORED),Some(new NumericDocValuesField(cd.name,value)))

  def setIndexValue(f: (Field,Option[Field]), value: Long, cd: ResourceProperty) {
    f._1.setLongValue(value)
    f._2.foreach(_.setLongValue(value))
  }
}