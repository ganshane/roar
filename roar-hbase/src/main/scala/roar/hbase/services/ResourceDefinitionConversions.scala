// Copyright 2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.services

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.lucene.document.Field
import roar.api.meta.ResourceDefinition.{ResourceProperty, ResourceTraitProperty}
import roar.api.meta.types._
import roar.api.meta.{ColumnType, DataColumnType, IndexType, ResourceDefinition}
import stark.utils.services.StarkException

import scala.util.control.NonFatal

/**
 * ResourceDefinition Conversions
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
 * @since 2015-03-01
 */
trait ResourceDefinitionConversions {
  implicit def resourceDefinitionWrapper(rd: ResourceDefinition) = new {
    private lazy val _categoryProperty:Option[(Int,ResourceProperty)] = findObjectColumn()
    /**
     * 加入一个资源属性
     */
    def addProperty(property: ResourceProperty) = rd.properties.add(property)

    /**
     * 加入一个动态特征资源属性
     */
    def addDynamicProperty(property: ResourceTraitProperty) = rd.dynamicType.properties.add(property)

    def categoryProperty = _categoryProperty
    private def findObjectColumn():Option[(Int,ResourceProperty)]={
      var ret:Option[(Int,ResourceProperty)] = None
      val it = rd.properties.iterator()
      var index = 0
      while(it.hasNext && ret.isEmpty){
        val rp = it.next()
        if(rp.objectCategory != null){
          ret = Some((index,rp))
        }
        index += 1
      }

      ret
    }
  }
  implicit def indexTypeWrapper(it: IndexType) = new {
    def indexType() = it match {
      case IndexType.Text =>
        Field.Index.ANALYZED
      case IndexType.Keyword =>
        Field.Index.NOT_ANALYZED
      case IndexType.UnIndexed =>
        Field.Index.NO
    }
  }

  implicit def wrapColumnType(ct: ColumnType) = new {
    def getColumnType: DataColumnType[_] = ct match {
      case ColumnType.Key=>
        KeyColumnType
      case ColumnType.Text=>
        TextColumnType
      case ColumnType.Date =>
        DateColumnType
      case ColumnType.Int =>
        IntColumnType
      case ColumnType.Long =>
        LongColumnType
    }
  }
  implicit def resourcePropertyOps(rp: ResourceProperty) = new {
    def createIndexField(value: Any): (Field,Option[Field]) = {
      rp.columnType.getColumnType.asInstanceOf[DataColumnType[Any]].createIndexField(value, rp)
    }

    def setIndexValue(f: (Field,Option[Field]), value: Any) {
      rp.columnType.getColumnType.asInstanceOf[DataColumnType[Any]].setIndexValue(f, value, rp)
    }

    def isToken: Boolean = {
      rp.columnType == ColumnType.Text
    }

    def isKeyword: Boolean = {
      rp.columnType == ColumnType.Key
    }

    def isNumeric: Boolean = {
      rp.columnType == ColumnType.Long || rp.columnType == ColumnType.Int || rp.columnType == ColumnType.Date
    }


    def readDfsValue(dbObj: Put) = {
      try {
        rp.columnType.getColumnType.readValueFromDfs(dbObj, rp)
      } catch {
        case NonFatal(e) =>
          throw new StarkException("unable to read value from dfs with name:" + rp.name, e, null)
      }
    }
    def readDfsValue(dbObj: Result) = {
      try {
        rp.columnType.getColumnType.readValueFromDfs(dbObj, rp)
      } catch {
        case NonFatal(e) =>
          throw new StarkException("unable to read value from dfs with name:" + rp.name, e, null)
      }
    }

    def readApiValue(dbObj: Result) = {
      rp.columnType.getColumnType.asInstanceOf[DataColumnType[Any]].convertDfsValueToString(readDfsValue(dbObj), rp)
    }
  }
}

object ResourceDefinitionConversions extends ResourceDefinitionConversions {}
