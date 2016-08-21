package roar.hbase.services

import org.apache.hadoop.hbase.client.{Delete, Durability, Get, Put}
import org.apache.hadoop.hbase.coprocessor.{BaseRegionObserver, CoprocessorException, ObserverContext, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.HRegion.Operation
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest
import org.apache.hadoop.hbase.regionserver.wal.{HLogKey, WALEdit}
import org.apache.hadoop.hbase.regionserver.{Store, StoreFile}
import org.apache.hadoop.hbase.{CellUtil, CoprocessorEnvironment, HRegionInfo}
import roar.hbase.internal.RegionSearchSupport
import stark.utils.services.LoggerSupport

/**
  * index region observer
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-03
  */
class IndexRegionObserver extends BaseRegionObserver
  with RegionIndexSupport
  with RegionSearchSupport
  with RegionCoprocessorEnvironmentSupport
  with IndexSearchServiceSupport
  with LoggerSupport{

  private var _env:RegionCoprocessorEnvironment = _

  override def coprocessorEnv: RegionCoprocessorEnvironment = _env


  override def postStartRegionOperation(ctx: ObserverContext[RegionCoprocessorEnvironment], op: Operation): Unit = super.postStartRegionOperation(ctx, op)

  override def postOpen(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    openIndexWriter()
    openSearcherManager()
  }


  override def preFlush(e: ObserverContext[RegionCoprocessorEnvironment]):Unit={
    prepareFlushIndex()
  }

  override def postFlush(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    waitForFlushIndexThreadFinished()
  }


  override def postCompact(e: ObserverContext[RegionCoprocessorEnvironment], store: Store, resultFile: StoreFile, request: CompactionRequest): Unit = {
  }

  override def postClose(e: ObserverContext[RegionCoprocessorEnvironment], abortRequested: Boolean): Unit = {
    closeSearcher()
    closeIndex()
    closeDirectory()
  }

  override def postPut(e: ObserverContext[RegionCoprocessorEnvironment], put: Put, edit: WALEdit, durability: Durability): Unit = {
    /**
      * 因为两次put针对不同的column,在put中并未包含全部的信息,
      * 如果仅仅使用put来进行索引,可能会导致丢失数据.
      * 这里使用一个Get从本Region区域找到全部数据进行索引
      */
    val get = new Get(put.getRow)
    /*
    if(put.getTimeStamp != HConstants.LATEST_TIMESTAMP){
      get.setTimeStamp(put.getTimeStamp)
    }
    */
    val result = _env.getRegion.get(get)

    index(put.getTimeStamp,result)
    maybeRefresh()
  }


  override def postDelete(e: ObserverContext[RegionCoprocessorEnvironment], delete: Delete, edit: WALEdit, durability: Durability): Unit = {
    deleteIndex(delete)
    maybeRefresh()
  }


  override def postWALRestore(env: ObserverContext[RegionCoprocessorEnvironment], info: HRegionInfo, logKey: HLogKey, logEdit: WALEdit): Unit = {
//  override def postWALRestore(env: ObserverContext[_ <: RegionCoprocessorEnvironment], info: HRegionInfo, logKey: WALKey, logEdit: WALEdit): Unit = {
    val cells = logEdit.getCells
    val it = cells.iterator()
    while(it.hasNext){
      val cell = it.next()
      val get = new Get(CellUtil.cloneRow(cell))

      val result = _env.getRegion.get(get)
      index(cell.getTimestamp,result)
    }
    maybeRefresh()
  }


  override def preSplit(c: ObserverContext[RegionCoprocessorEnvironment], splitRow: Array[Byte]): Unit = {
    //stop split
    if(maybeStopSplit)
      c.bypass()
  }

  override def start(e: CoprocessorEnvironment): Unit = {
    e match{
      case re: RegionCoprocessorEnvironment=>
        _env=re
      case _ =>
        throw new CoprocessorException("Must be loaded on a table region!")
    }
  }
}
