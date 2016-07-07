package roar.hbase.services

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.regionserver.{HRegion, RegionCoprocessorHost}
import org.apache.hadoop.hbase.util.{ByteStringer, Bytes}
import org.junit.{Assert, After, Before, Test}
import roar.hbase.model.ResourceDefinition
import roar.protocol.generated.RoarProtos.{SearchResponse, IndexSearchService, SearchRequest}
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

  private val xm = Bytes.toBytes("xm")
  private val row1 = Bytes.toBytes("r1")
  private var region:HRegion = _

  @Before
  def setup: Unit ={
    val conf = HBaseConfiguration.create()
//    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
//      classOf[IndexRegionObserver].getName)
    //    conf.setStrings(RoarHbaseConstants.ROAR_INDEX_HDFS_CONF_KEY,hdfsURI)
    util = new HBaseTestingUtility(conf)
//    util.startMiniCluster()
    val rd = XmlLoader.parseXML[ResourceDefinition](getClass.getResourceAsStream("/test_res.xml"), None)
    RegionServerData.regionServerResources = Map("czrk"->rd)

    val tableDesc = new HTableDescriptor(tableName)
    val colFamilyDesc = new HColumnDescriptor(family)
    tableDesc.addFamily(colFamilyDesc)
    tableDesc.addCoprocessor(classOf[IndexRegionObserver].getName)
    val regionInfo = new HRegionInfo(tableName, null, null, false);
    val regionPath = new Path("target/xx")
    region = HRegion.createHRegion(regionInfo, regionPath, conf, tableDesc);

    val coprocessorHost = new RegionCoprocessorHost(region, null, conf)
    region.setCoprocessorHost(coprocessorHost)
    region.getCoprocessorHost.load(classOf[IndexRegionObserver],1073741823,conf)
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
  def test_put: Unit ={
    val p = new Put(row1)
    p.addColumn(family, xm, xm)
    region.put(p)

    Assert.assertEquals(1,query("xm:xm").getCount)

    val delete = new Delete(row1)
    region.delete(delete)
    Assert.assertEquals(0,query("xm:xm").getCount)

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
