package roar.hbase.services

import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.zookeeper.ZKUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility}
import org.junit.{After, Assert, Before, Test}
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
    val resourcesPath = RegionServerData.resourcesPath
    val resPath = ZKUtil.joinZNode(resourcesPath,"czrk")
    debug("resPath:{}",resPath)
    while(RegionServerData.regionServerResources.isEmpty){
      ZKUtil.createSetData(zkw,resPath,bytes)
    }
    ZKUtil.setData(zkw,resPath,bytes)
    Assert.assertEquals(1,RegionServerData.regionServerResources.size)
    ZKUtil.setData(zkw,resPath,bytes)

    //test wrong define resource
    val wrongRes = IOUtils.toByteArray(getClass.getResourceAsStream("/test_res_wrong.xml"))
    val wrongResPath = ZKUtil.joinZNode(resourcesPath,"wrong_res")
    ZKUtil.createSetData(zkw,wrongResPath,wrongRes)

    ZKUtil.deleteNode(zkw,resPath)
    while(RegionServerData.regionServerResources.nonEmpty){
    }
    Assert.assertEquals(0,RegionServerData.regionServerResources.size)
  }
  @After
  def shutdown: Unit ={
    util.shutdownMiniCluster()
  }
}
