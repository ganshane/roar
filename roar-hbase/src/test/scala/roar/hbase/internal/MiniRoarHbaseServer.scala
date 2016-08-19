package roar.hbase.internal

import java.io.File

import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, HConstants}
import roar.hbase.services.IndexRegionObserver

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-08-18
  */
object MiniRoarHbaseServer {
  private var util:HBaseTestingUtility = _
  def main(args:Array[String]): Unit ={
    val conf = HBaseConfiguration.create()
//    conf.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
//      classOf[IndexRegionServerObserver].getName)
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      classOf[IndexRegionObserver].getName)
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT,3333)
    conf.setStrings(HConstants.ZOOKEEPER_ZNODE_PARENT,"/groups/test")
    //    conf.setStrings(RoarHbaseConstants.ROAR_INDEX_HDFS_CONF_KEY,hdfsURI)
    util = new HBaseTestingUtility(conf)
    val zkCluster = new MiniZooKeeperCluster(conf)
    zkCluster.setDefaultClientPort(3333)
    val zkDataPath: File = new File(conf.get(HConstants.ZOOKEEPER_DATA_DIR))
    System.out.println("zkDataPath:"+zkDataPath)
    zkCluster.startup(zkDataPath)

    util.setZkCluster(zkCluster)

    util.startMiniCluster()

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        util.shutdownMiniCluster()
      }
    }))

    Thread.currentThread().join()
  }
}
