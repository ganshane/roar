package roar.hbase.services

import org.apache.hadoop.hbase.client.{Durability, Put}
import org.apache.hadoop.hbase.coprocessor.{BaseRegionObserver, ObserverContext, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest
import org.apache.hadoop.hbase.regionserver.wal.WALEdit
import org.apache.hadoop.hbase.regionserver.{Region, Store, StoreFile}
import org.slf4j.LoggerFactory

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-03
  */
class IndexRegionObserver extends BaseRegionObserver{
  private val logger = LoggerFactory getLogger getClass

  override def postOpen(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    IndexSource.createIndex(e.getEnvironment)
  }



  override def postFlush(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    IndexSource.findIndex(e.getEnvironment.getRegionInfo.getRegionId).flush()
  }

  override def postCompact(e: ObserverContext[RegionCoprocessorEnvironment], store: Store, resultFile: StoreFile, request: CompactionRequest): Unit = {
  }

  override def postClose(e: ObserverContext[RegionCoprocessorEnvironment], abortRequested: Boolean): Unit = {
    logger.info("closing index writer...")
    IndexSource.findIndex(e.getEnvironment.getRegionInfo.getRegionId).close()
  }
  override def postPut(e: ObserverContext[RegionCoprocessorEnvironment], put: Put, edit: WALEdit, durability: Durability): Unit = {
    println("================>",toString)
    IndexSource.findIndex(e.getEnvironment.getRegionInfo.getRegionId).index(put.getFamilyCellMap)
  }

  override def postSplit(e: ObserverContext[RegionCoprocessorEnvironment], l: Region, r: Region): Unit = {
    val parent = e.getEnvironment.getRegion
    super.postSplit(e, l, r)
  }
}
