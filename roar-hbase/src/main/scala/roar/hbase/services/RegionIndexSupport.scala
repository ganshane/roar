package roar.hbase.services

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Delete, Result}
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.io.IOUtils
import org.apache.lucene.index._
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.BytesRef
import org.apache.solr.store.hdfs.HdfsDirectory
import roar.hbase.RoarHbaseConstants
import roar.hbase.model.ResourceDefinition
import stark.utils.services.LoggerSupport

/**
  * support region index
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
trait RegionIndexSupport {
  this:RegionCoprocessorEnvironmentSupport with LoggerSupport =>
  protected var indexWriterOpt:Option[IndexWriter] = None
  protected var rd:ResourceDefinition = _
  protected def openIndexWriter():Unit= {
    val enableIndex = coprocessorEnv.getRegion.getTableDesc.getConfigurationValue(RoarHbaseConstants.ENABLE_ROAR_INDEX_CONF_KEY)
    debug("=====> enableIndex",enableIndex)
    val tableName = coprocessorEnv.getRegion.getTableDesc.getTableName
    val regionEncodedName = coprocessorEnv.getRegionInfo.getEncodedName
    val resourceDefineOpt = RegionServerData.regionServerResources.get(tableName.getNameAsString)
    resourceDefineOpt match {
      case Some(rd) =>
        this.rd = rd
        val regionIndexPath = RoarHbaseConstants.REGION_INDEX_PATH_FORMAT.format(regionEncodedName)

        val rootDir = FSUtils.getRootDir(coprocessorEnv.getConfiguration)
        val tableDir = FSUtils.getTableDir(rootDir, tableName)

        val conf = new Configuration()
        val indexPath = new Path(tableDir, regionIndexPath)
        logger.info("create index with path {}", indexPath)

        val directory =
        if(indexPath.toString.startsWith("file"))
          FSDirectory.open(new File(indexPath.toUri).toPath)
        else
         new HdfsDirectory(indexPath, HdfsLockFactoryInHbase, conf)

        val config = new IndexWriterConfig(RoarHbaseConstants.defaultAnalyzer)
        val mergePolicy = new LogByteSizeMergePolicy()
        // compound files cannot be used with HDFS
        //    mergePolicy.setUseCompoundFile(false)
        config.setMergePolicy(mergePolicy)
        config.setMergeScheduler(new SerialMergeScheduler())
        indexWriterOpt = Some(new IndexWriter(directory, config))
      case None=>
        info("{} index not supported",tableName.getNameAsString)
    }
  }
  def index(timestamp:Long,result:Result): Unit = {
    indexWriterOpt foreach {indexWriter=>
      val rowTerm = createSIdTerm(result.getRow)
      debug("[{}] index row term {}",rd.name,rowTerm)
      val docOpt = RegionServerData.documentSource.newDocument(rd,timestamp,result)
      docOpt.foreach(indexWriter.updateDocument(rowTerm, _))
    }
  }
  def deleteIndex(delete:Delete):Unit={
    indexWriterOpt foreach {indexWriter=>
      val rowTerm = createSIdTerm(delete.getRow)
      debug("[{}] delete row term {}",rd.name,rowTerm)
      indexWriter.deleteDocuments(rowTerm)
      indexWriter.commit()
    }
  }
  private def createSIdTerm(id: Array[Byte]) = {
    val bb = new BytesRef(id)
    new Term(RoarHbaseConstants.OBJECT_ID_FIELD_NAME, bb)
  }
  protected def flushIndex(): Unit ={
    //提交索引到磁盘
    indexWriterOpt.foreach(_.commit())
  }
  protected def closeIndex():Unit={
    indexWriterOpt.foreach{indexWriter=>
      logger.info("closing index writer...")
      IOUtils.closeStream(indexWriter)
    }
  }
}

/**
  * provide RegionCoprocessor Environment
  */
trait RegionCoprocessorEnvironmentSupport{
  def coprocessorEnv:RegionCoprocessorEnvironment
}
