package roar.hbase.services

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Durability, Put}
import org.apache.hadoop.hbase.coprocessor.{BaseRegionObserver, ObserverContext, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest
import org.apache.hadoop.hbase.regionserver.wal.WALEdit
import org.apache.hadoop.hbase.regionserver.{Region, Store, StoreFile}
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, LogByteSizeMergePolicy, SerialMergeScheduler}
import org.apache.solr.store.hdfs.HdfsDirectory
import org.slf4j.LoggerFactory

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-03
  */
class IndexRegionObserver extends BaseRegionObserver{
  private val logger = LoggerFactory getLogger getClass
  private var indexWriter:IndexWriter = _
  private val pathFormat="INDEX/%s"

  override def postOpen(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    val tableName = e.getEnvironment.getRegion.getTableDesc.getTableName
    val encodedName = e.getEnvironment.getRegionInfo.getEncodedName
    val regionIndexPath = pathFormat.format(encodedName)

    val rootDir = FSUtils.getRootDir(e.getEnvironment.getConfiguration)
    val tableDir = FSUtils.getTableDir(rootDir,tableName)

    val conf=new Configuration()
    val indexPath = new Path(tableDir,regionIndexPath)
    logger.info("create index with path {}",indexPath)

    val directory = new HdfsDirectory(indexPath,HdfsLockFactoryInHbase,conf)
    val analyzer = new CJKAnalyzer
    val config=new IndexWriterConfig(analyzer)
    val mergePolicy = new LogByteSizeMergePolicy();
    // compound files cannot be used with HDFS
//    mergePolicy.setUseCompoundFile(false);
    config.setMergePolicy(mergePolicy);
    config.setMergeScheduler(new SerialMergeScheduler());
    indexWriter = new IndexWriter(directory,config)
  }


  override def postFlush(e: ObserverContext[RegionCoprocessorEnvironment]): Unit = {
    indexWriter.commit()
  }

  override def postCompact(e: ObserverContext[RegionCoprocessorEnvironment], store: Store, resultFile: StoreFile, request: CompactionRequest): Unit = {
    indexWriter.forceMerge(10,false)
  }

  override def postClose(e: ObserverContext[RegionCoprocessorEnvironment], abortRequested: Boolean): Unit = {
    logger.info("closing index writer...")
    indexWriter.flush()
    indexWriter.close()
  }
  override def postPut(e: ObserverContext[RegionCoprocessorEnvironment], put: Put, edit: WALEdit, durability: Durability): Unit = {
    println("================>",toString)
  }

  override def postSplit(e: ObserverContext[RegionCoprocessorEnvironment], l: Region, r: Region): Unit = {
    val parent = e.getEnvironment.getRegion
    super.postSplit(e, l, r)
  }
}
