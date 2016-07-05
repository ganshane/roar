package roar.hbase.services

import org.apache.hadoop.hbase.coprocessor.{MasterCoprocessorEnvironment, ObserverContext, BaseMasterObserver}

/**
  * index master observer
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-04
  */
class IndexMasterObserver extends BaseMasterObserver{
  override def preMasterInitialization(ctx: ObserverContext[MasterCoprocessorEnvironment]): Unit = {
    super.preMasterInitialization(ctx)
  }
}
