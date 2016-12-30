// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.api.meta.types

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Field, SortedDocValuesField, StringField}
import org.apache.lucene.util.BytesRef
import roar.api.meta.DataColumnType
import roar.api.meta.ResourceDefinition.ResourceProperty

/**
 * string column type
  *
  * @author jcai
 * @version 0.1
 */
object KeyColumnType extends KeyColumnType {
}

class KeyColumnType extends DataColumnType[String]{
  private final val GBK="GBK"

  override protected def convertCellAsData(cell: Cell,cd:ResourceProperty): Option[String] =
    Some(Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength))


  def convertDfsValueToString(value:Option[String],cd:ResourceProperty):Option[String]={
        value
  }
  override def createIndexField(value: String,cd:ResourceProperty):(Field,Option[Field]) = {
    (new StringField(cd.name, value, Store.NO),
      if(cd.sort) Some(new SortedDocValuesField(cd.name,new BytesRef(Bytes.toBytes(value))))
      else None
    )
  }
  def setIndexValue(f:(Field,Option[Field]),value:String,cd:ResourceProperty){
    f._1.asInstanceOf[Field].setStringValue(value)
    f._2.foreach(_.setBytesValue(Bytes.toBytes(value))) //setStringValue(value))
  }
}