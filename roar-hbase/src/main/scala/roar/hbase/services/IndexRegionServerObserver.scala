package roar.hbase.services

import java.util

import org.apache.hadoop.hbase.CoprocessorEnvironment
import org.apache.hadoop.hbase.coprocessor.{BaseRegionServerObserver, RegionServerCoprocessorEnvironment}
import org.apache.hadoop.hbase.zookeeper.{ZKUtil, ZooKeeperListener, ZooKeeperWatcher}
import roar.hbase.RoarHbaseConstants
import roar.hbase.model.ResourceDefinition
import roar.hbase.services.RegionServerData.ResourceListener
import stark.utils.StarkUtilsConstants
import stark.utils.services.{StarkException, LoggerSupport, XmlLoader}

import scala.collection.mutable.ArrayBuffer

/**
  * Region Server的Coprocessor
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
private[services] object RegionServerData extends LoggerSupport{
  @volatile
  var regionServerResources = Map[String, ResourceDefinition]()
  def addResources(zkw:ZooKeeperWatcher,resources:util.List[String]): Unit ={
    if(resources != null) {
      val it = resources.iterator()
      val buffer = new ArrayBuffer[ResourceDefinition](resources.size())
      while (it.hasNext) {
        val res = it.next()
        val resPath = ZKUtil.joinZNode(RoarHbaseConstants.RESOURCES_PATH, res)
        val data = ZKUtil.getDataAndWatch(zkw, resPath)
        val rdOpt = parseXML(data)
        rdOpt.foreach(buffer +=)
      }

      regionServerResources = buffer.map(x => (x.name, x)).toMap
    }
  }
  private def parseXML(data:Array[Byte]):Option[ResourceDefinition]={
    try {
      if(data == null) None
      else {
        val rd = XmlLoader.parseXML[ResourceDefinition](
          new String(data, StarkUtilsConstants.UTF8_ENCODING),
          xsd = Some(getClass.getResourceAsStream("/resource.xsd")))
        Some(rd)
      }
    }catch{
      case e:StarkException=>
        error("can't parse resource,msg:{}",e.getMessage)
        None
    }
  }
  class ResourceListener(zkw:ZooKeeperWatcher) extends ZooKeeperListener(zkw) {
    override def nodeChildrenChanged(path: String): Unit = {
      if(path == RoarHbaseConstants.RESOURCES_PATH){
        val resources = ZKUtil.listChildrenAndWatchThem(zkw,RoarHbaseConstants.RESOURCES_PATH)
        addResources(zkw,resources)
        debug("resources:{} children changed,children:{},size:{}",path,resources,regionServerResources.size)
      }
    }

    override def nodeCreated(path: String): Unit = {
      super.nodeCreated(path)
    }

    override def nodeDeleted(path: String): Unit = {
//      debug("resource:{} deleted",path)
      super.nodeDeleted(path)
    }

    override def nodeDataChanged(path: String): Unit = {
      if(path.startsWith(RoarHbaseConstants.RESOURCES_PATH)){
        debug("resource content changed:{}",path)
        val data = ZKUtil.getData(zkw,path)
        val rdOpt = parseXML(data)

        rdOpt foreach{rd=>
          val remain = regionServerResources - rd.name
          regionServerResources = remain + (rd.name->rd)
        }
      }
    }
  }
}
class IndexRegionServerObserver extends BaseRegionServerObserver with LoggerSupport{
  override def start(env: CoprocessorEnvironment): Unit = {
    debug("start region server coprocessor")
    val rss = env.asInstanceOf[RegionServerCoprocessorEnvironment].getRegionServerServices
    val zkw = rss.getZooKeeper
    zkw.registerListener(new ResourceListener(zkw))
    debug("watching {}",RoarHbaseConstants.RESOURCES_PATH)
    while(ZKUtil.checkExists(zkw,RoarHbaseConstants.RESOURCES_PATH) == -1){
      ZKUtil.createWithParents(zkw,RoarHbaseConstants.RESOURCES_PATH)
    }

    val resources = ZKUtil.listChildrenAndWatchThem(zkw,RoarHbaseConstants.RESOURCES_PATH)
    RegionServerData.addResources(zkw,resources)
    debug("finish start region server coprocessor,resource size:{}",RegionServerData.regionServerResources.size)
  }
}
