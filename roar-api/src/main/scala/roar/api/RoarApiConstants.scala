package roar.api

import org.apache.hadoop.hbase.util.Bytes

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-09-09
  */
object RoarApiConstants {
  //对象序列相关定义
  final val SEQ_FAMILY = Bytes.toBytes("_seq")
  final val SEQ_OID_QUALIFIER = Bytes.toBytes("oid")

}
