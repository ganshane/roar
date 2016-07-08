package roar.hbase.services

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Delete, Result}
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment
import org.apache.hadoop.hbase.regionserver.Region
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.io.IOUtils
import org.apache.lucene.index._
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.BytesRef
import org.apache.solr.store.hdfs.HdfsDirectory
import roar.hbase.RoarHbaseConstants
import roar.hbase.model.ResourceDefinition
import stark.utils.services.LoggerSupport

import scala.concurrent._
import scala.concurrent.duration.Duration
import ExecutionContext.Implicits.global


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
  private var flushIndexFuture:Future[Unit] = _
  private var splitterOpt:Option[Future[Unit]] = _

  protected def openIndexWriter():Unit= {
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
  def prepareFlushIndex(): Unit ={
    info("prepare flush index")
    this.flushIndexFuture = Future {
      flushIndex()
      info("finish flush index")
    }
  }
  def waitForFlushIndexThreadFinished(): Unit ={
    Await.result(this.flushIndexFuture,Duration.Inf)
  }
  def deleteIndex(delete:Delete):Unit={
    indexWriterOpt foreach {indexWriter=>
      val rowTerm = createSIdTerm(delete.getRow)
      debug("[{}] delete row term {}",rd.name,rowTerm)
      indexWriter.deleteDocuments(rowTerm)
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
  protected def prepareSplitIndex(splitRow:Array[Byte]): Unit ={
    splitterOpt = indexWriterOpt map {indexWriter=>
      info("prepare split index")
      val dir = indexWriter.getDirectory
      val splitter = dir match{
        case d:FSDirectory =>
          val pathA= new File(d.getDirectory.getParent.toFile,"d_a").toPath
          val pathB= new File(d.getDirectory.getParent.toFile,"d_b").toPath
          val dirA = FSDirectory.open(pathA)
          val dirB = FSDirectory.open(pathB)
          new PKIndexSplitter(d,dirA,dirB,createSIdTerm(splitRow))
        case d:HdfsDirectory=>
          val pathA=new Path(d.getHdfsDirPath.getParent,"d_a")
          val pathB=new Path(d.getHdfsDirPath.getParent,"d_b")
          val conf = d.getConfiguration
          val dirA = new HdfsDirectory(pathA, HdfsLockFactoryInHbase, conf)
          val dirB = new HdfsDirectory(pathB, HdfsLockFactoryInHbase, conf)
          new PKIndexSplitter(d,dirA,dirB,createSIdTerm(splitRow))
      }
      //create thread to split parent index
      Future[Unit] {
        splitter.split()
      }
    }
  }
  protected def rollbackSplitIndex(): Unit ={
    //TODO How to stop scala future (index thread).
    splitterOpt.foreach{f=>
    }
  }

  /**
    * wait split index thread finish
    */
  protected def awaitSplitIndexComplete(l:Region,r:Region): Unit ={
    splitterOpt.foreach{f=>
      Await.result(f,Duration.Inf)
      info("finish to split index")

      //rename directory
      indexWriterOpt foreach{ indexWriter=>
        val dir = indexWriter.getDirectory
        dir match{
          case d:FSDirectory =>
            val parentFile = d.getDirectory.getParent.toFile
            val pathA= new File(parentFile,"d_a")
            val pathADest= new File(parentFile,l.getRegionInfo.getEncodedName)
            FileUtils.moveDirectory(pathA,pathADest)
            val pathB= new File(parentFile,"d_b")
            val pathBDest= new File(parentFile,r.getRegionInfo.getEncodedName)
            FileUtils.moveDirectory(pathB,pathBDest)

          case d:HdfsDirectory=>
            val parent = d.getHdfsDirPath.getParent
            val pathA=new Path(parent,"d_a")
            val pathADest=new Path(parent,l.getRegionInfo.getEncodedName)
            val pathB=new Path(parent,"d_b")
            val pathBDest=new Path(parent,r.getRegionInfo.getEncodedName)
            parent.getFileSystem(d.getConfiguration).rename(pathA,pathADest)
            parent.getFileSystem(d.getConfiguration).rename(pathB,pathBDest)
        }
      }
      //move directory
    }
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
