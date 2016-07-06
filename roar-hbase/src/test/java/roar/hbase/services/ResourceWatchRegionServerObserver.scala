package roar.hbase.services

import org.apache.hadoop.hbase.CoprocessorEnvironment
import org.apache.hadoop.hbase.coprocessor.{BaseRegionServerObserver, RegionServerCoprocessorEnvironment}
import org.apache.hadoop.hbase.zookeeper.ZKUtil

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
class ResourceWatchRegionServerObserver extends BaseRegionServerObserver{
  override def start(env: CoprocessorEnvironment): Unit = {
    val rss = env.asInstanceOf[RegionServerCoprocessorEnvironment].getRegionServerServices
    val zkw = rss.getZooKeeper
    ZKUtil.listChildrenAndWatchThem(zkw,"/monad/test/resource")
    super.start(env)
  }
}
