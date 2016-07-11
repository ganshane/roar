package roar.hbase.services

import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.zookeeper.ZKUtil
import org.junit.{After, Before}
import roar.hbase.RoarHbaseConstants
import stark.utils.services.LoggerSupport

/**
  * first hbase test case
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-11
  */
class BaseIntegrationTestCase extends LoggerSupport{
  protected var util:HBaseTestingUtility = _
  protected val tableName = TableName.valueOf("czrk")
  protected val family = Bytes.toBytes("info")

  @Before
  def setup: Unit ={
    val conf = HBaseConfiguration.create()
    conf.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      classOf[IndexRegionServerObserver].getName)
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      classOf[IndexRegionObserver].getName)

    util = new HBaseTestingUtility(conf)
    util.startMiniCluster()

    //create resource definition
    val zkw = util.getZooKeeperWatcher
    val bytes = IOUtils.toByteArray(getClass.getResourceAsStream("/test_res.xml"))
    val parent = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT)
    val resourcesPath = ZKUtil.joinZNode(parent,RoarHbaseConstants.RESOURCES_PATH)
    val resPath = ZKUtil.joinZNode(resourcesPath,tableName.getNameAsString)
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

}
