package roar.hbase.services

import java.util.{List, Map}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment
import org.apache.hadoop.hbase.util.{Bytes, FSUtils}
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.index._
import org.apache.lucene.util.BytesRef
import org.apache.solr.store.hdfs.HdfsDirectory
import roar.hbase.RoarHbaseConstants
import stark.utils.services.LoggerSupport

/**
  * support region index
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
trait RegionIndexSupport {
  this:RegionCoprocessorEnvironmentSupport with LoggerSupport =>
  protected var indexWriter:IndexWriter = _
  protected def openIndexWriter():Unit= {
    val tableName = coprocessorEnv.getRegion.getTableDesc.getTableName
    val encodedName = coprocessorEnv.getRegionInfo.getEncodedName
    val regionIndexPath = RoarHbaseConstants.REGION_INDEX_PATH_FORMAT.format(encodedName)

    val rootDir = FSUtils.getRootDir(coprocessorEnv.getConfiguration)
    val tableDir = FSUtils.getTableDir(rootDir,tableName)

    val conf=new Configuration()
    val indexPath = new Path(tableDir,regionIndexPath)
    logger.info("create index with path {}",indexPath)

    val directory = new HdfsDirectory(indexPath,HdfsLockFactoryInHbase,conf)
    val config=new IndexWriterConfig(RoarHbaseConstants.defaultAnalyzer)
    val mergePolicy = new LogByteSizeMergePolicy()
    // compound files cannot be used with HDFS
    //    mergePolicy.setUseCompoundFile(false)
    config.setMergePolicy(mergePolicy)
    config.setMergeScheduler(new SerialMergeScheduler())
    indexWriter = new IndexWriter(directory,config)
  }
  def index(rowArray:Array[Byte],familyMap:Map[Array[Byte], List[Cell]]): Unit = {
    val row = new BytesRef(rowArray)
    val rowTerm = new Term(RoarHbaseConstants.ROW_FIELD, row)
    //遍历所有的列族
    val doc = RoarHbaseConstants.DEFAULT_TRANSFER.transform(row,familyMap)
    if (doc != null) {
      indexWriter.updateDocument(rowTerm, doc)
    }
  }
}
class DefaultDocumentTransformer {
  /*
  @Override
  def transform(row:BytesRef,kvs:List[KeyValue]):Document={
    var row:Array[Byte] = null
    var timestamp:Long = -1
    val doc = new Document()
    val it =kvs.iterator()
    while(it.hasNext){
      val kv = it.next()
      if (row == null) {
        row = kv.getRow()
        timestamp = kv.getTimestamp()
      }
      val name = Bytes.toString(kv.getQualifier())
      val value = new String(kv.getValue())
      val field = new Field(name, value, Store.NO, Index.ANALYZED)
      doc.add(field)
    }
//    addFields(row, timestamp, doc)
    return doc
  }
  */

  def transform(row:BytesRef,familyMap:Map[Array[Byte], List[Cell]]):Document={
    var timestamp:Long = -1
    val doc = new Document()
    var added = false
    val it = familyMap.entrySet().iterator()
    while(it.hasNext){
      val entry = it.next
      val value = entry.getValue
      //val family = Bytes.toString(key)
      val valueIt = value.iterator()
      while(valueIt.hasNext){
        val kv = valueIt.next()
        timestamp = kv.getTimestamp
        val name = Bytes.toString(kv.getQualifierArray,kv.getQualifierOffset,kv.getQualifierLength)
        val value = kv.getValueArray
        val field = new StringField(name, new BytesRef(value, kv.getValueOffset,kv.getValueLength),Store.NO)
        doc.add(field)
        added = true
      }
    }
    if (!added) {
      return null
    }
    addFields(row, timestamp, doc)
    return doc
  }

  private def addFields(row:BytesRef, timestamp:Long, doc:Document) {
    val rowField = new StringField(RoarHbaseConstants.ROW_FIELD, row, Store.YES)
    doc.add(rowField)
    val timestampField = new LongField(RoarHbaseConstants.TIMESTAMP_FIELD, timestamp, Store.YES)
    doc.add(timestampField)
  }

}

/**
  * provide RegionCoprocessor Environment
  */
trait RegionCoprocessorEnvironmentSupport{
  def coprocessorEnv:RegionCoprocessorEnvironment
}
