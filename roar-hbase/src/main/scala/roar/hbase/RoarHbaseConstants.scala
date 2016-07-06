package roar.hbase

import org.apache.lucene.analysis.cjk.CJKAnalyzer
import roar.hbase.services.DefaultDocumentTransformer

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-03
  */
object RoarHbaseConstants {
  final val REGION_INDEX_PATH_FORMAT= "INDEX/%s"
  final val ROW_FIELD = "_row"
  final val TIMESTAMP_FIELD = "_ut" // rowid_timestamp
  final val defaultAnalyzer = new CJKAnalyzer

  final val DEFAULT_TRANSFER=new DefaultDocumentTransformer
  final val ENABLE_ROAR_INDEX_CONF_KEY="roar.index.enabled"

}
