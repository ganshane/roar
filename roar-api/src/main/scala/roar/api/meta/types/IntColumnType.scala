// Copyright 2011,2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.api.meta.types

import java.nio.ByteBuffer
import java.sql.PreparedStatement

import org.apache.hadoop.hbase.Cell
import org.apache.lucene.document.{Field, IntField, NumericDocValuesField}
import roar.api.meta.DataColumnType
import roar.api.meta.ResourceDefinition.ResourceProperty

/**
 * Int Type Column
 *
 * @author jcai
 * @version 0.1
 */

object IntColumnType extends IntColumnType

class IntColumnType extends DataColumnType[Int] {


  override protected def convertCellAsData(cell: Cell,cd:ResourceProperty): Option[Int] = {
    Some(ByteBuffer.wrap(cell.getValueArray,cell.getValueOffset,cell.getValueLength).getInt)
  }

  def convertDfsValueToString(value: Option[Int], cd: ResourceProperty) =
    if (value.isDefined) Some(value.get.toString) else None

  def setJdbcParameter(ps: PreparedStatement, index: Int, value: Int, cd: ResourceProperty) {
    ps.setInt(index, value)
  }

  def createIndexField(value: Int, cd: ResourceProperty) =
    (new IntField(cd.name, value, IntField.TYPE_NOT_STORED), if(cd.sort) Some(new NumericDocValuesField(cd.name, value)) else None)

  def setIndexValue(f: (Field,Option[Field]), value: Int, cd: ResourceProperty) {
    f._1.setIntValue(value)
    f._2.foreach(_.setLongValue(value))
  }
}