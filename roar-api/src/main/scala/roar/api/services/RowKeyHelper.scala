package roar.api.services

import org.apache.hadoop.hbase.util.Bytes
import org.apache.lucene.util.BytesRefBuilder
import roar.api.meta.ObjectCategory

/**
  * row key helper for hbase
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-09-09
  */
object RowKeyHelper {
  /**
    * 得到记录序列 -> id号码的映射
    *
    * @return id
    */
  def buildIdSeqRowKey(regionStartKeyBytes:Array[Byte],category: ObjectCategory,idSeq:Int):Array[Byte]={
    val idCardSeqKey = new BytesRefBuilder()
    val regionStartKey = getRegionStartKey(regionStartKeyBytes)

    idCardSeqKey.append(regionStartKey,0,regionStartKey.length)
    val categoryBytes = Bytes.toBytes(category.ordinal())
    idCardSeqKey.append(categoryBytes,0,categoryBytes.length)
    val seqBytes = Bytes.toBytes(idSeq)
    idCardSeqKey.append(seqBytes,0,seqBytes.length)

    idCardSeqKey.bytes
  }
  def getRegionStartKey(regionStartKeyBytes:Array[Byte]): Array[Byte] = {
    var regionStartKey = regionStartKeyBytes
    if (regionStartKey == null || regionStartKey.isEmpty)
      regionStartKey = Bytes.toBytes("00000000") //default start Key
    regionStartKey
  }
}
