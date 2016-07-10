package roar.hbase.services

import java.util

import org.apache.hadoop.hbase.{HConstants, CoprocessorEnvironment}
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, BaseRegionServerObserver, RegionServerCoprocessorEnvironment}
import org.apache.hadoop.hbase.zookeeper.{ZKUtil, ZooKeeperListener, ZooKeeperWatcher}
import roar.hbase.RoarHbaseConstants
import roar.hbase.internal.DocumentSourceImpl
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
  //create document source
  val documentSource = new DocumentSourceImpl(new java.util.HashMap[String, DocumentCreator]())
  var resourcesPath:String = _

  def addResources(zkw:ZooKeeperWatcher,resources:util.List[String]): Unit ={
    if(resources != null) {
      val it = resources.iterator()
      val buffer = new ArrayBuffer[ResourceDefinition](resources.size())
      while (it.hasNext) {
        val res = it.next()
        val resPath = ZKUtil.joinZNode(resourcesPath, res)
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
      if(path == resourcesPath){
        val resources = ZKUtil.listChildrenAndWatchThem(zkw,resourcesPath)
        addResources(zkw,resources)
        debug("resources:{} children changed,children:{},size:{}",path,resources,regionServerResources.size)
      }
    }

    //called when resource definition changed
    override def nodeDataChanged(path: String): Unit = {
      if(path.startsWith(resourcesPath)){
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
    env match {
      case rssEnv: RegionServerCoprocessorEnvironment =>
        val parent = env.getConfiguration.get(HConstants.ZOOKEEPER_ZNODE_PARENT)
        val resourcesPath = ZKUtil.joinZNode(parent,RoarHbaseConstants.RESOURCES_PATH)
        info("start region index server coprocessor")
        RegionServerData.resourcesPath = resourcesPath
        val rss = rssEnv.getRegionServerServices
        val zkw = rss.getZooKeeper
        zkw.registerListener(new ResourceListener(zkw))
        info("watching {}", resourcesPath)
        while (ZKUtil.checkExists(zkw, resourcesPath) == -1) {
          ZKUtil.createWithParents(zkw, resourcesPath)
        }

        val resources = ZKUtil.listChildrenAndWatchThem(zkw, resourcesPath)
        RegionServerData.addResources(zkw, resources)
        info("finish start region server coprocessor,resource size:{}", RegionServerData.regionServerResources.size)
      case _ =>
        throw new CoprocessorException("Must be loaded on a region server!")
    }
  }
}
