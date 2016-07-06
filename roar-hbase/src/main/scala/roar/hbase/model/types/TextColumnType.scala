// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
/*
 * Copyright 2012 The EGF IT Software Department.
 */

package roar.hbase.model.types

import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.TextField
import roar.hbase.model.AnalyzerCreator
import roar.hbase.model.ResourceDefinition.ResourceProperty

/**
 * clob类型的列
 *
 * @author jcai
 */

object TextColumnType extends TextColumnType

class TextColumnType extends KeyColumnType {
  override def createIndexField(value: String,cd:ResourceProperty) = {
    val f = if(cd.analyzer != null){
      val analyzer = AnalyzerCreator.create(cd.analyzer)
      new TextField(cd.name, analyzer.tokenStream(cd.name,value))
    }else{
      new TextField(cd.name, value, Store.NO)
    }
    (f,None)
  }
}
