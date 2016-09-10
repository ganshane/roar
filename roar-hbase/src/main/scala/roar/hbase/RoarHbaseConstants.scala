package roar.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.analysis.cjk.CJKAnalyzer

/**
  * define some constants
 *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-03
  */
object RoarHbaseConstants {
  final val REGION_INDEX_PATH_FORMAT= "INDEX/%s"
  final val defaultAnalyzer = new CJKAnalyzer

  final val RESOURCES_PATH="resources"
  final val INDEX_TRANSACTIONS_PATH="region-idx-in-split" //index transactions

  /* 记录了主键字段,支持查询以及快速获取 */
  final val ROW_ID_FIELD_NAME = "_id"
  final val UPDATE_TIME_FIELD_NAME = "_ut"

//  final val OBJECT_ID_PAYLOAD_FIELD = "_PL"
//  final val OBJECT_ID_PAYLOAD_VALUE = "_UID"
  final val DELETED_SID = java.lang.Integer.MIN_VALUE
  //进行频次搜索时候，输出结果中的次数
  final val FACET_COUNT = "_count"
  final val DYNAMIC_DESC = "_desc"


  //支持的资源定义
  final val TRACE_RESOURCE="trace" //轨迹
  final val BEHAVIOUR_RESOURCE="behaviour" //行为

  //对象序列相关定义
  final val SEQ_INC_QUALIFIER = Bytes.toBytes("inc")
  final val SEQ_QUALIFIER = Bytes.toBytes("seq")
}
