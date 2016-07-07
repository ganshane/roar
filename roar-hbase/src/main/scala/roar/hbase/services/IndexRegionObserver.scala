package roar.hbase.services

import org.apache.hadoop.hbase.client.{Delete, Durability, Get, Put}
import org.apache.hadoop.hbase.coprocessor.{BaseRegionObserver, ObserverContext, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest
import org.apache.hadoop.hbase.regionserver.wal.WALEdit
import org.apache.hadoop.hbase.regionserver.{Region, Store, StoreFile}
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

  override def postOpen(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    _env = e.getEnvironment
    openIndexWriter()
    openSearcherManager()
  }

  override def postFlush(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    flushIndex()
  }

  override def postCompact(e: ObserverContext[RegionCoprocessorEnvironment], store: Store, resultFile: StoreFile, request: CompactionRequest): Unit = {
  }

  override def postClose(e: ObserverContext[RegionCoprocessorEnvironment], abortRequested: Boolean): Unit = {
    closeSearcher()
    closeIndex()
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

    index(put,result)
    mybeRefresh()
  }


  override def postDelete(e: ObserverContext[RegionCoprocessorEnvironment], delete: Delete, edit: WALEdit, durability: Durability): Unit = {
    deleteIndex(delete)
    mybeRefresh()
  }

  override def postSplit(e: ObserverContext[RegionCoprocessorEnvironment], l: Region, r: Region): Unit = {
  }
}
