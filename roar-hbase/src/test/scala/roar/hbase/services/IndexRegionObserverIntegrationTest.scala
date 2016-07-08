package roar.hbase.services

import java.io.IOException

import junit.framework.Assert._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch.{Call, Callback}
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.ipc.{BlockingRpcCallback, ServerRpcController}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.zookeeper.ZKUtil
import org.junit.{After, Assert, Before, Test}
import roar.hbase.RoarHbaseConstants
import roar.protocol.generated.RoarProtos.{IndexSearchService, SearchRequest, SearchResponse}
import stark.utils.services.LoggerSupport

/**
  * first hbase test case
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-06-29
  */
class IndexRegionObserverIntegrationTest extends LoggerSupport{
  private var util:HBaseTestingUtility = _
  private val tableName = TableName.valueOf("czrk")
  private val family = Bytes.toBytes("info")

  private val xm = Bytes.toBytes("xm")
  private val row1 = Bytes.toBytes("r1")

  @Before
  def setup: Unit ={
    val conf = HBaseConfiguration.create()
    conf.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      classOf[IndexRegionServerObserver].getName)
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      classOf[IndexRegionObserver].getName)
//    conf.setStrings(RoarHbaseConstants.ROAR_INDEX_HDFS_CONF_KEY,hdfsURI)
    util = new HBaseTestingUtility(conf)
    util.startMiniCluster()

    //create resource definition
    val zkw = util.getZooKeeperWatcher
    val bytes = IOUtils.toByteArray(getClass.getResourceAsStream("/test_res.xml"))
    val resPath = ZKUtil.joinZNode(RoarHbaseConstants.RESOURCES_PATH,tableName.getNameAsString)
    debug("resPath:{}",resPath)
    while(RegionServerData.regionServerResources.isEmpty){
      ZKUtil.createSetData(zkw,resPath,bytes)
    }
    debug("resource loaded for path:{}",resPath)

    val admin = util.getHBaseAdmin()
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName)
      }
      admin.deleteTable(tableName)
    }

    util.createTable(tableName, Array[Array[Byte]](family))
  }
  @After
  def tearDown: Unit ={
    util.shutdownMiniCluster()
  }

  @Test
  def testSimple: Unit ={

    val t = util.getConnection.getTable(tableName)
    val p = new Put(row1)
    p.addColumn(family, xm, xm)
    // before HBASE-4331, this would throw an exception
    t.put(p)

    val request = SearchRequest.newBuilder()
    request.setQ("xm:xm")

    //test proxy on region server
    var list = List[SearchResponse]()
    t.coprocessorService(classOf[IndexSearchService],null,null,
      new Call[IndexSearchService,SearchResponse](){
        override def call(instance: IndexSearchService): SearchResponse = {
          val controller: ServerRpcController = new ServerRpcController
          val rpcCallback  = new BlockingRpcCallback[SearchResponse]
          instance.query(controller,request.build(),rpcCallback)
          if (controller.failedOnException) {
            throw controller.getFailedOn
          }
          rpcCallback.get
        }
      },new Callback[SearchResponse] {
        override def update(region: Array[Byte], row: Array[Byte], result: SearchResponse): Unit = {
          list = result :: list
        }
      })
    Assert.assertEquals(1,list.size)

    val channel = t.coprocessorService(row1)
    val service = IndexSearchService.newBlockingStub(channel)
    val response = service.query(null,request.build())
    Assert.assertEquals(1,response.getCount)
    Assert.assertArrayEquals(row1,response.getRow(0).getRowId.toByteArray)

    t.close()


    util.deleteTable(tableName)
//    Thread.currentThread().join()
  }

  @throws(classOf[IOException])
  private def checkRowAndDelete(t: Table, row: Array[Byte], count: Int) {
    val g: Get = new Get(row)
    val r: Result = t.get(g)
    assertEquals(count, r.size)
    val d: Delete = new Delete(row)
    t.delete(d)
  }
  /*
  private def checkRowAndDelete(t:Table , row:Array[Byte], count:Int){
    val g = new Get(row)
    val r = t.get(g)
    Assert.assertEquals(count, r.size())
    val d = new Delete(row)
    t.delete(d)
  }
  */
}
