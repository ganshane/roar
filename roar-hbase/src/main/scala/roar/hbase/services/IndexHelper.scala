package roar.hbase.services

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.lucene.index.Term
import org.apache.lucene.store.{Directory, FSDirectory}
import org.apache.lucene.util.BytesRef
import org.apache.solr.store.hdfs.HdfsDirectory
import roar.hbase.RoarHbaseConstants

/**
  * index helper class
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-11
  */
object IndexHelper {
  def createSIdTerm(id: Array[Byte]) = {
    val bb = new BytesRef(id)
    new Term(RoarHbaseConstants.OBJECT_ID_FIELD_NAME, bb)
  }
  def getDaughtersIndexTmpDir(parent: HRegionInfo, conf: Configuration):(Directory,Directory) = {
    val parentPath = getIndexPath(parent,conf)
    val dirA = createDirectoryInstance(new Path(parentPath,"d_a"),conf)
    val dirB = createDirectoryInstance(new Path(parentPath,"d_b"),conf)
    (dirA,dirB)
  }
  def getIndexPath(regionInfo:HRegionInfo,conf:Configuration): Path = {
    val regionIndexPath = RoarHbaseConstants.REGION_INDEX_PATH_FORMAT.format(regionInfo.getEncodedName)

    val rootDir = FSUtils.getRootDir(conf)
    val tableDir = FSUtils.getTableDir(rootDir, regionInfo.getTable)

    new Path(tableDir, regionIndexPath)
  }
  def getIndexDirectory(regionInfo:HRegionInfo,conf:Configuration): Directory= {
    val indexPath = getIndexPath(regionInfo,conf)
    createDirectoryInstance(indexPath,conf)
  }
  private def createDirectoryInstance(indexPath:Path,conf:Configuration): Directory ={
    if(indexPath.toString.startsWith("file"))
      FSDirectory.open(new File(indexPath.toUri).toPath)
    else
      new HdfsDirectory(indexPath, HdfsLockFactoryInHbase, conf)
  }
}
