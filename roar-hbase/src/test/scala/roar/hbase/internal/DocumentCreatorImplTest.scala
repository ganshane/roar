package roar.hbase.internal

import java.io.{Closeable, File}
import java.util

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortedSetSortField}
import org.junit.{After, Assert, Before, Test}
import org.mockito.{Matchers, Mockito}
import roar.api.meta.ResourceDefinition
import roar.api.services.RoarSparseFixedBitSet
import roar.hbase.services._
import stark.utils.services.{LoggerSupport, XmlLoader}

/**
  * test
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-08-18
  */
class DocumentCreatorImplTest {

  @Test
  def test_trace: Unit ={
    val result = Mockito.mock(classOf[Result])
    Mockito.when(result.getRow).thenReturn("first".getBytes())
    val region = searcher.coprocessorEnv.getRegion
    Mockito.when(region.get(Matchers.any(classOf[Get]))).thenReturn(result)
    searcher.index(1L,result.getRow)
    Assert.assertEquals(0,searcher.numDoc)

    createCell(result,"object_id","123")
    createCell(result,"start_time",1234)
    createCell(result,"end_time",1237)
    createCell(result,"trace_type","test")

    searcher.index(1L,result.getRow)

    Mockito.when(result.getRow).thenReturn("2".getBytes())
    createCell(result,"object_id","321")
    searcher.index(1L,result.getRow)

    searcher.maybeRefresh()
    searcher.prepareFlushIndex()
    searcher.waitForFlushIndexThreadFinished()

    Mockito.when(result.getRow).thenReturn("3".getBytes())
    createCell(result,"object_id","321")
    searcher.index(1L,result.getRow)
    searcher.maybeRefresh()



    Assert.assertEquals(3,searcher.numDoc)



    searcher.doInSearcher{s=>
      val q = "object_id:123"
      val query = searcher.parseQuery(q)
      val sort = new Sort();
      sort.setSort(new SortedSetSortField("object_id", false))
      val topDocs = s.search(query,10,sort)
      Assert.assertEquals(1,topDocs.totalHits)
    }

    var idResultOpt = searcher.searchObjectId("123")
    idResultOpt.foreach{idResult=>
      val idBitSet = RoarSparseFixedBitSet.deserialize(idResult.getData.newInput())
      Assert.assertEquals(1,idBitSet.cardinality())
      Assert.assertTrue(idBitSet.get(0))
    }
    idResultOpt = searcher.searchObjectId("321")
    idResultOpt.foreach{idResult=>
      val idBitSet = RoarSparseFixedBitSet.deserialize(idResult.getData.newInput())
      Assert.assertEquals(1,idBitSet.cardinality())
      Assert.assertTrue(idBitSet.get(1))
    }
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
  var traceRd:ResourceDefinition  = _

  var indexWriter:IndexWriter =  _

  var documentSource:DocumentSource  = _

  var searcher:RegionSearchSupport with QueryParserSupport with RegionIndexSupport with RegionCoprocessorEnvironmentSupport with Closeable = _

  @Before
  def setup: Unit ={

    traceRd = XmlLoader.parseXML[ResourceDefinition](getClass.getResourceAsStream("/trace.xml"), None)
    RegionServerData.regionServerResources = Map("trace"->traceRd)
    documentSource = new DocumentSourceImpl(new util.HashMap[String,DocumentCreator]())
    /*
    val conf = new IndexWriterConfig(RoarHbaseConstants.defaultAnalyzer)
    val ramDir = new RAMDirectory()
    indexWriter = RandomIndexWriter.mockIndexWriter(ramDir,conf,new Random)
    */


    searcher = new RegionSearchSupport with QueryParserSupport with RegionIndexSupport with RegionCoprocessorEnvironmentSupport with LoggerSupport with Closeable{
      val env = Mockito.mock(classOf[RegionCoprocessorEnvironment])
      val currentRegion = Mockito.mock(classOf[HRegion])
      Mockito.when(env.getRegion).thenReturn(currentRegion)
      val tableDesc = Mockito.mock(classOf[HTableDescriptor])
      Mockito.when(region.getTableDesc).thenReturn(tableDesc)
      Mockito.when(tableDesc.getTableName).thenReturn(TableName.valueOf("trace"))

      val regionInfo = Mockito.mock(classOf[HRegionInfo])
      Mockito.when(env.getRegionInfo).thenReturn(regionInfo)
      Mockito.when(regionInfo.getEncodedName).thenReturn("trace")

      val conf = HBaseConfiguration.create()
      Mockito.when(env.getConfiguration).thenReturn(conf)
      conf.set(HConstants.HBASE_DIR,"target/hbase-test")

      @inline
      override def coprocessorEnv: RegionCoprocessorEnvironment = env

      def close() = getSearcherManager.foreach(_.close)
    }

    searcher.openIndexWriter()
    searcher.openSearcherManager()
  }
  @After
  def teardown: Unit ={
    searcher.closeSearcher()
    searcher.closeIndex()
    FileUtils.deleteQuietly(new File("target/hbase-test"))
  }
}
