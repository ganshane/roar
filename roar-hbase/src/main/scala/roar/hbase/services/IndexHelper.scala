package roar.hbase.services

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HRegionInfo}
import org.apache.hadoop.hbase.client.{Increment, Get}
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.util.{Bytes, FSUtils}
import org.apache.lucene.index.Term
import org.apache.lucene.store.{Directory, FSDirectory}
import org.apache.lucene.util.{BytesRefBuilder, BytesRef}
import org.apache.solr.store.hdfs.HdfsDirectory
import roar.api.meta.ObjectCategory
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
  /**
    * find object id sequence number by ObjectId
    *
    * @return sequence number
    */
  private[hbase] def findObjectIdSeq(region:HRegion,objectId:Array[Byte],category:ObjectCategory): Int ={
    import RoarHbaseConstants._
    val brb = new BytesRefBuilder()
    brb.append(region.getStartKey,0,region.getStartKey.length)
    val categoryBytes = Bytes.toBytes(category.ordinal())
    brb.append(categoryBytes,0,categoryBytes.length)
    brb.append(objectId,0,objectId.length)

    val get = new Get(brb.toBytesRef.bytes)
    val result = region.get(get)


    if(result != null && !result.isEmpty){//found seq
      val seqCell = result.getColumnCells(SEQ_FAMILY,SEQ_QUALIFIER).get(0)
      val seqBytes = CellUtil.cloneValue(seqCell)
      Bytes.toInt(seqBytes)
    }else{ //seq not found,so increment
    //TODO HOW TO LOCK CURRENT Process
    val categorySeqRowKey = new BytesRefBuilder
      categorySeqRowKey.append(region.getStartKey,0,region.getStartKey.length)
      categorySeqRowKey.append(categoryBytes,0,categoryBytes.length)
      val inc = new Increment(categorySeqRowKey.toBytesRef.bytes)
      inc.addColumn(SEQ_FAMILY,SEQ_INC_QUALIFIER,1)
      val result = region.increment(inc)
      val seqCell = result.getColumnCells(SEQ_FAMILY,SEQ_INC_QUALIFIER).get(0)
      val seqBytes = CellUtil.cloneValue(seqCell)
      Bytes.toLong(seqBytes).toInt
    }
  }
}
