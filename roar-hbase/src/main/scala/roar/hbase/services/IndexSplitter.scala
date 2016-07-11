package roar.hbase.services


import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse
import org.apache.hadoop.hbase.zookeeper.{ZKUtil, ZooKeeperListener, ZooKeeperWatcher}
import org.apache.hadoop.hbase.{HConstants, HRegionInfo}
import org.apache.lucene.index.PKIndexSplitter
import org.apache.zookeeper.CreateMode
import roar.hbase.RoarHbaseConstants
import stark.utils.services.LoggerSupport

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * split index thread
  *
  * 1. watch index split request folder in zookeeper
  * 2. split index
  * 3. notify region to load the new index
  * 4. remove parent directory and subdirectory
  * 5. complete the transaction
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-10
  */
object IndexSplitter extends LoggerSupport{
  var future:Future[Unit] = _
  def getTransactionPath(conf:Configuration):String={
    val parent = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT)
    ZKUtil.joinZNode(parent,RoarHbaseConstants.INDEX_TRANSACTIONS_PATH)
  }
  def isIndexSplitting(zkw:ZooKeeperWatcher,conf:Configuration): Boolean={
    val transactionPath = getTransactionPath(conf)
    //TODO check node version to avoid concurrent request?
    ZKUtil.nodeHasChildren(zkw,transactionPath)
  }
  def submitSplit(zkw:ZooKeeperWatcher,path:String,conf:Configuration):Future[Unit]={
    future = Future[Unit]{
      val daughterAPath = ZKUtil.joinZNode(getTransactionPath(conf),path)
      val data = ZKUtil.getData(zkw,daughterAPath)

      /**
        *  the regions include parent,daughterA and daughterB
        */
      val regions = GetOnlineRegionResponse.newBuilder().mergeFrom(data).build()
      if(regions.getRegionInfoCount != 3){
        throw new RuntimeException("wrong regions content,regions number must be 3")
      }

      val parent  = HRegionInfo.convert(regions.getRegionInfo(0))
      val daughterA = HRegionInfo.convert(regions.getRegionInfo(1))
      val daughterB = HRegionInfo.convert(regions.getRegionInfo(2))

      val parentDir = IndexHelper.getIndexDirectory(parent,conf)
      //TODO async to split
      //      val (daughterADir,daughterBDir) = IndexHelper.getDaughtersIndexTmpDir(parent,conf)
      val daughterADir = IndexHelper.getIndexDirectory(daughterA,conf)
      val daughterBDir = IndexHelper.getIndexDirectory(daughterB,conf)

      val midTerm = IndexHelper.createSIdTerm(daughterB.getStartKey)
      val splitter = new PKIndexSplitter(parentDir,daughterADir,daughterBDir,midTerm)
      info("begin to split index {} as [{},{}]",parent.getEncodedName,daughterA.getEncodedName,daughterB.getEncodedName)
      splitter.split()
      info("finish to split index {} as [{} {}]",parent.getEncodedName,daughterA.getEncodedName,daughterB.getEncodedName)
      //need to notify daughterA and daughterB to merge
    }

    future
  }
}
class WatchIndexSplitTransaction(zkw:ZooKeeperWatcher,conf:Configuration)
  extends ZooKeeperListener(zkw) with LoggerSupport{

  private val indexSplitPath = IndexSplitter.getTransactionPath(conf)
  //register listener
  zkw.registerListener(this)

  //watch transaction path
  ZKUtil.createNodeIfNotExistsNoWatch(zkw,indexSplitPath,null,CreateMode.PERSISTENT)
  val children = ZKUtil.listChildrenAndWatchThem(zkw,indexSplitPath)
  processChildren(children)

  override def nodeChildrenChanged(path: String): Unit = {
    if(path == indexSplitPath){
      val children = ZKUtil.listChildrenNoWatch(zkw,path)
      processChildren(children)
    }
  }
  def processChildren(children:util.List[String]): Unit ={
    if(children == null){
      return
    }
    if(children.size() > 1){
      throw new RuntimeException("multi index split request,%s".format(children))
    }
    if(children.size() > 0){
      IndexSplitter.submitSplit(zkw,children.get(0),conf)
    }
  }
}
