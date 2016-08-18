package roar.hbase.internal

import java.util

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.junit.{Assert, Test}
import org.mockito.Mockito
import roar.api.meta.ResourceDefinition
import roar.hbase.services.DocumentCreator
import stark.utils.services.XmlLoader

/**
  * test
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-08-18
  */
class DocumentCreatorImplTest {
  @Test
  def test_trace: Unit ={
    val traceRd = XmlLoader.parseXML[ResourceDefinition](getClass.getResourceAsStream("/trace.xml"), None)
    val createorSource = new DocumentSourceImpl(new util.HashMap[String,DocumentCreator]())
    val result = Mockito.mock(classOf[Result])
    var docOpt = createorSource.newDocument(traceRd,101L,result)
    Assert.assertTrue(docOpt.isEmpty)

    Mockito.when(result.getRow).thenReturn("first".getBytes())
    createCell(result,"object_id",123)
    createCell(result,"start_time",1234)
    createCell(result,"end_time",1237)
    createCell(result,"trace_type","test")

    docOpt = createorSource.newDocument(traceRd,101L,result)
    Assert.assertTrue(docOpt.isDefined)

  }
  private def createCell(result: Result,field:String,value:Int): Cell ={
    val v = Bytes.toBytes(value)
    createCell(result, field, v)
  }

  private def createCell(result: Result, field: String, v: Array[Byte]): Cell = {
    val cell = Mockito.mock(classOf[Cell])
    Mockito.when(cell.getValueArray).thenReturn(v)
    Mockito.when(cell.getValueLength).thenReturn(v.length)
    Mockito.when(cell.getValueOffset).thenReturn(0)
    Mockito.when(result.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes(field))).thenReturn(cell)
    cell
  }

  private def createCell(result: Result, field:String, value:String): Cell={
    val v = Bytes.toBytes(value)
    createCell(result, field, v)
  }
}
