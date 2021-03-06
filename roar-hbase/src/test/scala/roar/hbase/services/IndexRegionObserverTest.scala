package roar.hbase.services

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Get, Delete, Put}
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.regionserver.{HRegion, RegionCoprocessorHost}
import org.apache.hadoop.hbase.util.{ByteStringer, Bytes}
import org.junit.{After, Assert, Before, Test}
import roar.api.RoarApiConstants
import roar.api.RoarApiConstants._
import roar.api.meta.{ObjectCategory, ResourceDefinition}
import roar.api.services.RowKeyHelper
import roar.protocol.generated.RoarProtos.{IndexSearchService, SearchRequest, SearchResponse}
import stark.utils.services.XmlLoader

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-07
  */
class IndexRegionObserverTest {
  private var util:HBaseTestingUtility = _
  private val tableName = TableName.valueOf("czrk")
  private val family = Bytes.toBytes("info")

  private val row1 = Bytes.toBytes("r1")
  private val xm = Bytes.toBytes("xm")
  private val xb = Bytes.toBytes("xb")
  private var region:HRegion = _
  private var conf:Configuration = _

  @Before
  def setup: Unit ={
    conf = HBaseConfiguration.create()
    util = new HBaseTestingUtility(conf)
    val rd = XmlLoader.parseXML[ResourceDefinition](getClass.getResourceAsStream("/test_res.xml"), None)
    RegionServerData.regionServerResources = Map("czrk"->rd)

    val tableDesc = new HTableDescriptor(tableName)
    val colFamilyDesc = new HColumnDescriptor(family)
    tableDesc.addFamily(colFamilyDesc)

    val seqFamilyDesc = new HColumnDescriptor(RoarApiConstants.SEQ_FAMILY)
    tableDesc.addFamily(seqFamilyDesc)

    tableDesc.addCoprocessor(classOf[IndexRegionObserver].getName)
    val regionInfo = new HRegionInfo(tableName, null, null, false);
    val regionPath = new Path("target/xx")
    region = HRegion.createHRegion(regionInfo, regionPath, conf, tableDesc);

    val coprocessorHost = new RegionCoprocessorHost(region, null, conf)
    region.setCoprocessorHost(coprocessorHost)
    coprocessorHost.postOpen()
  }
  @After
  def tearDown: Unit ={
    region.close()
  }
  private def query(q:String):SearchResponse={
    val request = SearchRequest.newBuilder()
    request.setQ(q)
    val method = IndexSearchService.getDescriptor.findMethodByName("query")
    val call = ClientProtos.CoprocessorServiceCall.newBuilder
      .setRow(ByteStringer.wrap(HConstants.EMPTY_BYTE_ARRAY))
      .setServiceName(method.getService.getFullName)
      .setMethodName(method.getName)
      .setRequest(request.build().toByteString).build

    region.execService(null,call).asInstanceOf[SearchResponse]
  }
  @Test
  def test_findObjectId: Unit ={
    var seq = IndexHelper.findObjectIdSeq(region)(Bytes.toBytes("asdf"),ObjectCategory.Mac)
    Assert.assertEquals(1,seq)
    Assert.assertEquals(1,IndexHelper.findCurrentSeq(region,ObjectCategory.Mac))

    seq = IndexHelper.findObjectIdSeq(region)(Bytes.toBytes("asdf"),ObjectCategory.Mac)
    Assert.assertEquals(1,seq)
    Assert.assertEquals(1,IndexHelper.findCurrentSeq(region,ObjectCategory.Mac))

    seq = IndexHelper.findObjectIdSeq(region)(Bytes.toBytes("fdsa"),ObjectCategory.Mac)
    Assert.assertEquals(2,seq)
    Assert.assertEquals(2,IndexHelper.findCurrentSeq(region,ObjectCategory.Mac))

    val rowId = RowKeyHelper.buildIdSeqRowKey(region.getStartKey,ObjectCategory.Mac,2)
    val get = new Get(rowId)
    val result = region.get(get)
    val cell = result.getColumnLatestCell(SEQ_FAMILY,SEQ_OID_QUALIFIER)
    val values = CellUtil.cloneValue(cell)
    Assert.assertEquals("fdsa",Bytes.toString(values))
  }
  @Test
  def test_put: Unit ={
    var p = new Put(row1)
    p.add(family, xm, xm)
    region.put(p)

    Assert.assertEquals(1,query("xm:xm").getCount)

    p = new Put(row1)
    p.add(family, xb, xb)
    region.put(p)
    Assert.assertEquals(1,query("xb:xb").getCount)
    //当再次put某个字段的时候,xm未被索引
    Assert.assertEquals(1,query("xm:xm").getCount)
    val delete = new Delete(row1)
    region.delete(delete)
    Assert.assertEquals(0,query("xm:xm").getCount)

    //test flush
    region.flushcache() ;//.flush(true)

    /*
    val channel = t.coprocessorService(row1)
    val service = IndexSearchService.newBlockingStub(channel)
    val request = SearchRequest.newBuilder()
    request.setQ("xm:xm")
    val response = service.query(null,request.build())
    Assert.assertEquals(1,response.getCount)
    Assert.assertArrayEquals(row1,response.getRow(0).getRowId.toByteArray)
    region.close()
    */
  }
}
