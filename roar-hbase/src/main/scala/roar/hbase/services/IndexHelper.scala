package roar.hbase.services

import java.io.File
import java.util.concurrent.locks.ReentrantLock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter.MutationReplay
import org.apache.hadoop.hbase.{HConstants, CellUtil, HRegionInfo}
import org.apache.hadoop.hbase.client.{Put, Increment, Get}
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.util.{Bytes, FSUtils}
import org.apache.lucene.index.Term
import org.apache.lucene.store.{Directory, FSDirectory}
import org.apache.lucene.util.{BytesRefBuilder, BytesRef}
import org.apache.solr.store.hdfs.HdfsDirectory
import roar.api.meta.ObjectCategory
import roar.api.services.RowKeyHelper
import roar.hbase.RoarHbaseConstants
import roar.hbase.RoarHbaseConstants._
import roar.api.RoarApiConstants._

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
    * 得到记录序列 -> id号码的映射
    *
    * @return id
    */
  def buildIdSeqRowKey(region:HRegion,category: ObjectCategory,idSeq:Int):Array[Byte]={
    RowKeyHelper.buildIdSeqRowKey(region.getStartKey,category,idSeq)
  }
  private def getRegionStartKey(region:HRegion): Array[Byte] = {
    RowKeyHelper.getRegionStartKey(region.getStartKey)
  }
  /**
    * find object id sequence number by ObjectId
    *
    * @return sequence number
    */
  private[hbase] def findCurrentSeq(region:HRegion,category:ObjectCategory): Int ={

    val regionStartKey = getRegionStartKey(region)
    val categoryBytes = Bytes.toBytes(category.ordinal())
    val categorySeqRowKey = new BytesRefBuilder
    categorySeqRowKey.append(regionStartKey, 0, regionStartKey.length)
    categorySeqRowKey.append(categoryBytes, 0, categoryBytes.length)
    val get = new Get(categorySeqRowKey.toBytesRef.bytes)
    get.addColumn(SEQ_FAMILY,SEQ_INC_QUALIFIER)
    val result = region.get(get)
    val seqCell = result.getColumnLatestCell(SEQ_FAMILY, SEQ_INC_QUALIFIER)
    val seqBytes = CellUtil.cloneValue(seqCell)
    Bytes.toLong(seqBytes).toInt
  }
  private[hbase] def findObjectIdSeq(region:HRegion)(objectId:Array[Byte],category:ObjectCategory): Int ={
    import RoarHbaseConstants._


    val idCardSeqKey = new BytesRefBuilder()
    val regionStartKey = getRegionStartKey(region)
    idCardSeqKey.append(regionStartKey,0,regionStartKey.length)
    val categoryBytes = Bytes.toBytes(category.ordinal())
    idCardSeqKey.append(categoryBytes,0,categoryBytes.length)
    idCardSeqKey.append(objectId,0,objectId.length)


    val get = new Get(idCardSeqKey.toBytesRef.bytes)
    var incResult = region.get(get)

    if(incResult != null && !incResult.isEmpty){//found seq
      val seqCell = incResult.getColumnLatestCell(SEQ_FAMILY,SEQ_QUALIFIER)
      val seqBytes = CellUtil.cloneValue(seqCell)
      Bytes.toInt(seqBytes)
    }else{ //seq not found,so increment
      try {
        lock.lock()
        //find sequence again
        incResult = region.get(get)
        if(incResult != null && !incResult.isEmpty) {
          //found seq
          val seqCell = incResult.getColumnLatestCell(SEQ_FAMILY, SEQ_QUALIFIER)
          val seqBytes = CellUtil.cloneValue(seqCell)
          Bytes.toLong(seqBytes).toInt
        }else {
          val categorySeqRowKey = new BytesRefBuilder
          categorySeqRowKey.append(regionStartKey, 0, regionStartKey.length)
          categorySeqRowKey.append(categoryBytes, 0, categoryBytes.length)

          val inc = new Increment(categorySeqRowKey.toBytesRef.bytes)
          inc.addColumn(SEQ_FAMILY, SEQ_INC_QUALIFIER, 1)
          val result = region.increment(inc)
          val seqCell = result.getColumnLatestCell(SEQ_FAMILY, SEQ_INC_QUALIFIER)
          val seqBytes = CellUtil.cloneValue(seqCell)
          val seq = Bytes.toLong(seqBytes).toInt


          /**
            * 写入两行,分别是:
            * seq->objectId
            * objectId->seq
            */
          val put = new Put(buildIdSeqRowKey(region,category,seq))
          put.add(SEQ_FAMILY,SEQ_OID_QUALIFIER,objectId)
          val put2 = new Put(idCardSeqKey.bytes())
          put2.add(SEQ_FAMILY,SEQ_QUALIFIER,Bytes.toBytes(seq))
          val replay1 = new MutationReplay(MutationType.PUT,put,HConstants.NO_NONCE, HConstants.NO_NONCE)
          val replay2 = new MutationReplay(MutationType.PUT,put2,HConstants.NO_NONCE, HConstants.NO_NONCE)
          //此处不适用batchMutation,因为此处的数据不进行索引,避免多余coprocessor开销
          region.batchReplay(Array(replay1,replay2))

//          println("create new seq :",seq,Bytes.toString(objectId))
          seq

        }
      }finally{
        lock.unlock()
      }
    }
  }
  private val lock = new ReentrantLock()
}
