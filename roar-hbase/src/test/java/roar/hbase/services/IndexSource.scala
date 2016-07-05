package roar.hbase.services

import java.util.concurrent.ConcurrentHashMap
import java.util.{List, Map}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment
import org.apache.hadoop.hbase.util.{Bytes, FSUtils}
import org.apache.hadoop.util.StringUtils
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.document.Field.{Index, Store}
import org.apache.lucene.document.{Document, Field}
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{SearcherFactory, SearcherManager, IndexSearcher}
import org.apache.lucene.util.BytesRef
import org.apache.solr.store.hdfs.HdfsDirectory
import org.slf4j.LoggerFactory

/**
  * global index source
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-05
  */
object IndexSource {
  final val indexes = new ConcurrentHashMap[Long,RegionIndexer]()
  final val pathFormat = "INDEX/%s"
  final val ROW_FIELD = "row";
  final val UID_FIELD = "uid"; // rowid_timestamp
  final val defaultAnalyzer = new CJKAnalyzer
  final val documentTransformer = new DefaultDocumentTransformer
  def toUID(row:String, timestamp:Long):String= {
    row + "_" + timestamp;
  }
  private def getRowTerm(kv:Cell):Term= {
    val row = new BytesRef(kv.getRow());
    new Term(ROW_FIELD, row)
  }
  def getRowTerm(familyMap:Map[Array[Byte], List[Cell]] ):Term= {
    val it = familyMap.entrySet().iterator()
    if(it.hasNext) getRowTerm(it.next().getValue.get(0))
    else null
  }
  def createIndex(env:RegionCoprocessorEnvironment): RegionIndexer ={
    val id = env.getRegionInfo.getRegionId
    var index =  indexes.get(id)
    if(index!=null)
      throw new RuntimeException("index already created with id "+id)

    index = new RegionIndexer(env)
    indexes.put(id,index)

    index

  }
  def findIndex(regionId:Long):RegionIndexer={
    indexes.get(regionId)
  }
}

class RegionIndexer(env:RegionCoprocessorEnvironment){
  def flush(): Unit = writer.commit()


  private val logger = LoggerFactory getLogger getClass

  private val region = env.getRegion
  private val (writer,searcherManager)= initIndex()


  private def initIndex()= {
    val tableName = env.getRegion.getTableDesc.getTableName
    val encodedName = env.getRegionInfo.getEncodedName
    val regionIndexPath = IndexSource.pathFormat.format(encodedName)

    val rootDir = FSUtils.getRootDir(env.getConfiguration)
    val tableDir = FSUtils.getTableDir(rootDir,tableName)

    val conf=new Configuration()
    val indexPath = new Path(tableDir,regionIndexPath)
    logger.info("create index with path {}",indexPath)

    val directory = new HdfsDirectory(indexPath,HdfsLockFactoryInHbase,conf)
    val config=new IndexWriterConfig(IndexSource.defaultAnalyzer)
    val mergePolicy = new LogByteSizeMergePolicy();
    // compound files cannot be used with HDFS
    //    mergePolicy.setUseCompoundFile(false);
    config.setMergePolicy(mergePolicy);
    config.setMergeScheduler(new SerialMergeScheduler());
    val writer = new IndexWriter(directory,config)

    val searcherManager = new SearcherManager(writer,new SearcherFactory())
    (writer,searcherManager)
  }
  def index(familyMap:Map[Array[Byte], List[Cell]]): Unit ={
    val rowTerm = IndexSource.getRowTerm(familyMap)
    val doc = IndexSource.documentTransformer.transform(familyMap)
    if (doc != null ) {
      //System.out.println("LuceneCoprocessor.postPut term:"+rowTerm+" "+doc);
      if (rowTerm != null) {
        writer.updateDocument(rowTerm, doc);
      } else {
        writer.addDocument(doc);
      }
    }
    searcherManager.maybeRefresh()
  }
  def search(q:String,offset:Int=0,limit:Int=30): scala.List[Document]={
    var searcher: IndexSearcher = null
    try {
      searcher = searcherManager.acquire()
      println("max doc:"+searcher.getIndexReader.maxDoc())
      val parser = new QueryParser("id",IndexSource.defaultAnalyzer)
      val query = parser.parse(q)
      val docs = searcher.search(query,offset+limit)
      if(docs.scoreDocs.length > offset) {
        docs.scoreDocs.drop(offset).map{scoreDoc =>
          convert(searcher.doc(scoreDoc.doc))
        }.toList
      } else Nil
    }finally {
      searcherManager.release(searcher)
    }
  }

  private def convert(d:Document):Document= {
    val uidField = d.getField(IndexSource.UID_FIELD);
    val uid = uidField.stringValue();
    val split = StringUtils.split(uid, '?', '_');
    val row = Bytes.toBytes(split(0));
    val timestamp = split(1).toLong
    val get = new Get(row);
    get.setTimeStamp(timestamp);
    val doc = new Document();
    val result = region.get(get, false);
    val size = result.size()
    Range(0,size).foreach{i=>
      val cell = result.get(i)
        val keyStr2 = Bytes.toString(cell.getQualifierArray);
        val valStr2 = Bytes.toString(cell.getValueArray);
//        System.out.println("keyStr:"+""+" keyStr2:"+keyStr2+" valStr2:"+valStr2);
        val field = new Field(keyStr2, valStr2, Store.YES, Index.ANALYZED);
        doc.add(field);
    }
    doc.add(uidField);
    val rowField = new Field("row", split(0), Store.YES, Index.NOT_ANALYZED);
    doc.add(rowField);
    //System.out.println(d + "");
    return doc;
  }

  def close(): Unit = {
    searcherManager.close()
    IOUtils.closeQuietly(writer)
  }
}
class DefaultDocumentTransformer {
  @Override
  def transform(kvs:List[KeyValue]):Document={
    var row:Array[Byte] = null
    var timestamp:Long = -1;
    val doc = new Document();
    val it =kvs.iterator()
    while(it.hasNext){
      val kv = it.next();
      if (row == null) {
        row = kv.getRow();
        timestamp = kv.getTimestamp();
      }
      val name = Bytes.toString(kv.getQualifier());
      val value = new String(kv.getValue());
      val field = new Field(name, value, Store.NO, Index.ANALYZED);
      doc.add(field);
    }
    addFields(row, timestamp, doc);
    return doc;
  }

  def transform(familyMap:Map[Array[Byte], List[Cell]]):Document={
    var row:Array[Byte] = null;
    var timestamp:Long = -1;
    val doc = new Document();
    var added = false;
    val it = familyMap.entrySet().iterator()
    while(it.hasNext){
      val entry = it.next
      val key = entry.getKey
      val value = entry.getValue
      val family = Bytes.toString(key);
      // if (!family.equals("info")) {
//      System.out.println("family:" + family);
      val valueIt = value.iterator()
      while(valueIt.hasNext){
        val kv = valueIt.next()
//        System.out.println("key:"+Bytes.toString(kv.getQualifierArray()))
        if (row == null) {
          row = kv.getRow();
          val rowStr = Bytes.toString(row);
//          System.out.println("rowstr:" + rowStr);
          timestamp = kv.getTimestamp();
        }
        val name = Bytes.toString(kv.getQualifier());
        // String name = kv.getKeyString();
        val value = new String(kv.getValue());
//        System.out.println("name:"+name+" value:"+value)
        val field = new Field(name, value, Store.NO, Index.ANALYZED);
        doc.add(field);
        added = true;
      }
      // }
    }
    if (!added) {
      return null;
    }
    addFields(row, timestamp, doc);
    return doc;
  }

  private def addFields(row:Array[Byte], timestamp:Long, doc:Document) {
    val rowStr = new String(row);
    val rowField = new Field(IndexSource.ROW_FIELD, rowStr, Store.NO,Index.NOT_ANALYZED);
    doc.add(rowField);
    val uid = IndexSource.toUID(rowStr, timestamp);
    val uidField = new Field(IndexSource.UID_FIELD, uid, Store.YES,Index.NOT_ANALYZED);
    doc.add(uidField);
  }

}

