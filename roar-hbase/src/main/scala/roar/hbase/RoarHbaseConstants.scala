package roar.hbase

import org.apache.lucene.analysis.cjk.CJKAnalyzer

/**
  * define some constants
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-03
  */
object RoarHbaseConstants {
  final val REGION_INDEX_PATH_FORMAT= "INDEX/%s"
  final val defaultAnalyzer = new CJKAnalyzer

  final val RESOURCES_PATH="/resources"
  final val RESOURCE_PATH_FORMAT=RESOURCES_PATH+"/%s"

  /*Index Constant*/
  final val OBJECT_ID_FIELD_NAME = "_id"
  final val UPDATE_TIME_FIELD_NAME = "_ut"
  final val OID_FILED_NAME= "_OID"

  final val OBJECT_ID_PAYLOAD_FIELD = "_PL"
  final val OBJECT_ID_PAYLOAD_VALUE = "_UID"
  final val DELETED_SID = java.lang.Integer.MIN_VALUE
  //进行频次搜索时候，输出结果中的次数
  final val FACET_COUNT = "_count"
  final val DYNAMIC_DESC = "_desc"

}
