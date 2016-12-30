package roar.api.meta

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.lucene.document.Field
import roar.api.meta.ResourceDefinition.ResourceProperty

/**
  * 数据列定义类型
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
trait DataColumnType[T] {
  /**
    * 从分布式数据库中获得数据
    *
    * @param row 行数据
    * @param cd 对应列的定义
    */
  def readValueFromDfs(row: Result, cd: ResourceProperty) = {
    val cell = row.getColumnLatestCell(cd.family.getBytes,cd.name.getBytes)
    if(cell == null)
      None
    else
      convertCellAsData(cell,cd)
  }

  protected def convertCellAsData(cell: Cell,cd:ResourceProperty): Option[T]

  def readValueFromDfs(row: Put, cd: ResourceProperty): Option[T] = {
    val cellList = row.get(cd.family.getBytes,cd.name.getBytes)
    if(cellList.size()==0) None
    else convertCellAsData(cellList.get(0),cd)
  }
  /**
    * 从分布式数据中读取数据用来供API显示
    *
    * @param value 行数据
    * @param cd 对应列的定义
    */
  def convertDfsValueToString(value:Option[T],cd:ResourceProperty):Option[String]
  /**
    * 创建索引字段对象
    */
  def createIndexField(value: T,cd:ResourceProperty):(Field,Option[Field])

  /**
    * 设置索引字段的值
    *
    * @param f 字段定义
    * @param value 值
    * @param cd 属性定义
    */
  def setIndexValue(f:(Field,Option[Field]),value:T,cd:ResourceProperty)

}
