package roar.hbase.services

import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.zookeeper.ZKUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility}
import org.junit.{Assert, After, Before, Test}
import roar.hbase.RoarHbaseConstants
import stark.utils.services.LoggerSupport

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
class IndexRegionServerObserverTest extends LoggerSupport{
  private var util:HBaseTestingUtility = _
  @Before
  def setup: Unit ={
    val conf = HBaseConfiguration.create()
    conf.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      classOf[IndexRegionServerObserver].getName)
    util = new HBaseTestingUtility(conf)
    util.startMiniCluster()
  }
  @Test
  def test_res: Unit ={
    val zkw = util.getZooKeeperWatcher
    val bytes = IOUtils.toByteArray(getClass.getResourceAsStream("/test_res.xml"))
    val resPath = ZKUtil.joinZNode(RoarHbaseConstants.RESOURCES_PATH,"czrk")
    debug("resPath:{}",resPath)
    while(RegionServerData.regionServerResources.isEmpty){
      ZKUtil.createSetData(zkw,resPath,bytes)
    }
    val res = RegionServerData.regionServerResources.get("czrk").get
    Assert.assertEquals("czrk",res.name)
  }
  @After
  def shutdown: Unit ={
    util.shutdownMiniCluster()
  }
}
