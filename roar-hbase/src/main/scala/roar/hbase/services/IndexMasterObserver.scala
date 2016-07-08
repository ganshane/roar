package roar.hbase.services

import org.apache.hadoop.hbase.coprocessor._
import org.apache.hadoop.hbase.{CoprocessorEnvironment, HRegionInfo, HTableDescriptor}

/**
  * index master observer
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-04
  */
class IndexMasterObserver extends BaseMasterObserver{
  private var env:MasterCoprocessorEnvironment = _
  override def preCreateTable(ctx: ObserverContext[MasterCoprocessorEnvironment], desc: HTableDescriptor, regions: Array[HRegionInfo]): Unit =
    super.preCreateTable(ctx, desc, regions)

  override def preCreateTableHandler(ctx: ObserverContext[MasterCoprocessorEnvironment], desc: HTableDescriptor, regions: Array[HRegionInfo]): Unit = super.preCreateTableHandler(ctx, desc, regions)

  override def preMasterInitialization(ctx: ObserverContext[MasterCoprocessorEnvironment]): Unit = {
    super.preMasterInitialization(ctx)
  }

  override def start(ctx: CoprocessorEnvironment): Unit = {
    ctx match{
      case mce: MasterCoprocessorEnvironment =>
        env=mce
      case _ =>
        throw new CoprocessorException("Must be loaded on a master server!")
    }
  }
}
