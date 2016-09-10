// Copyright 2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.api.meta

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.lucene.document.Field.Index
import org.apache.lucene.document.{Field, NumericDocValuesField}
import roar.api.RoarApiConstants
import roar.api.meta.ResourceDefinition.{ResourceProperty, ResourceTraitProperty}
import roar.api.meta.types._

import scala.language.implicitConversions
import scala.util.control.NonFatal

/**
 * ResourceDefinition Conversions
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
 * @since 2015-03-01
 */
trait ResourceDefinitionConversions {
  trait ResourceDefinitionWrapper{
    def addProperty(property: ResourceProperty):Unit
    def addDynamicProperty(property: ResourceTraitProperty):Unit
    def categoryProperty:Option[(Int,ResourceProperty)]
  }
  implicit def resourceDefinitionWrapper(rd: ResourceDefinition):ResourceDefinitionWrapper = new ResourceDefinitionWrapper {
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

  trait IndexTypeWrapper{
    def indexType():Index
  }
  implicit def indexTypeWrapper(it: IndexType):IndexTypeWrapper = new IndexTypeWrapper {
    def indexType() = it match {
      case IndexType.Text =>
        Field.Index.ANALYZED
      case IndexType.Keyword =>
        Field.Index.NOT_ANALYZED
      case IndexType.UnIndexed =>
        Field.Index.NO
    }
  }
  trait ColumnTypeWrapper{
    def getColumnType: DataColumnType[_]
  }

  implicit def wrapColumnType(ct: ColumnType):ColumnTypeWrapper= new ColumnTypeWrapper{
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
      case other=>
        throw new RuntimeException("column type not supported "+other)

    }
  }
  trait ResourcePropertyWrapper{

    def createIndexField(value: Any): (Field,Option[Field])
    def setIndexValue(f: (Field,Option[Field]), value: Any):Unit
    def isToken: Boolean
    def isKeyword: Boolean
    def isNumeric: Boolean
    def readDfsValue(dbObj: Put):Option[Any]
    def readDfsValue(dbObj: Result):Option[Any]
    def readDfsValueAsByteArray(dbObj: Result):Option[Array[Byte]]
    def readApiValue(dbObj: Result):Option[String]
    def createObjectIdField():Field
  }
  implicit def resourcePropertyOps(rp: ResourceProperty):ResourcePropertyWrapper = new ResourcePropertyWrapper{
    def createIndexField(value: Any): (Field,Option[Field]) = {
      rp.columnType.getColumnType.asInstanceOf[DataColumnType[Any]].createIndexField(value, rp)
    }


    override def readDfsValueAsByteArray(dbObj: Result): Option[Array[Byte]] = {
      val cell = dbObj.getColumnLatestCell(rp.family.getBytes,rp.name.getBytes)
      if(cell == null)
        None
      else
        Some(CellUtil.cloneValue(cell))
    }

    override def createObjectIdField(): Field = {
      new NumericDocValuesField(RoarApiConstants.OBJECT_ID_FIELD_FORMAT.format(rp.name),1)
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
          throw new RuntimeException("unable to read value from dfs with name:" + rp.name, e)
      }
    }
    def readDfsValue(dbObj: Result) = {
      try {
        rp.columnType.getColumnType.readValueFromDfs(dbObj, rp)
      } catch {
        case NonFatal(e) =>
          throw new RuntimeException("unable to read value from dfs with name:" + rp.name, e)
      }
    }

    def readApiValue(dbObj: Result) = {
      rp.columnType.getColumnType.asInstanceOf[DataColumnType[Any]].convertDfsValueToString(readDfsValue(dbObj), rp)
    }
  }
}

object ResourceDefinitionConversions extends ResourceDefinitionConversions {}
