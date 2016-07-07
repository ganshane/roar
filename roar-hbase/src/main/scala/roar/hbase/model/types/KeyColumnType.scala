// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.model.types

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Field, StringField}
import roar.hbase.model.ResourceDefinition.ResourceProperty
import roar.hbase.model.{AnalyzerCreator, DataColumnType}

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

  override protected def convertCellAsData(cell: Cell): Option[String] =
    Some(Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength))


  def convertDfsValueToString(value:Option[String],cd:ResourceProperty):Option[String]={
        value
  }
  override def createIndexField(value: String,cd:ResourceProperty):(Field,Option[Field]) = {
    (new StringField(cd.name, value, Store.NO),None)
  }
  def setIndexValue(f:(Field,Option[Field]),value:String,cd:ResourceProperty){
    //TODO 此处频繁创建analyzer,
    if(cd.analyzer != null) {
      val analyzer = AnalyzerCreator.create(cd.analyzer)
      f._1.asInstanceOf[Field].setTokenStream(analyzer.tokenStream(cd.name,value))
    }else {
      f._1.asInstanceOf[Field].setStringValue(value)
    }
  }
}