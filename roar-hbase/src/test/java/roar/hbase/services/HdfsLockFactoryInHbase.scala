package roar.hbase.services

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.store.{Directory, Lock, LockFactory, LockReleaseFailedException}
import org.apache.solr.store.hdfs.{HdfsLockFactory, HdfsDirectory}

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-05
  */
object HdfsLockFactoryInHbase extends LockFactory {
  val instance=HdfsLockFactory.INSTANCE
  override def obtainLock(dir: Directory, lockName: String): Lock = {
    instance.obtainLock(dir,lockName)
    val hdfsDir = dir.asInstanceOf[HdfsDirectory]
    val conf: Configuration = hdfsDir.getConfiguration
    val lockPath: Path = hdfsDir.getHdfsDirPath
    val lockFile: Path = new Path(lockPath, lockName)
    return new Lock {
      override def ensureValid(): Unit ={}
      override def close(): Unit = {
        val fs: FileSystem = FileSystem.get(lockFile.toUri, conf)
        try {
          if (fs.exists(lockFile) && !fs.delete(lockFile, false)) {
            throw new LockReleaseFailedException("failed to delete: " + lockFile)
          }
        } finally {
          //因为在hbase的region目录里面创建的文件系统,所以此处不要关闭文件系统
          //否则出现整个文件系统被关闭的错误
//          IOUtils.closeQuietly(fs)
        }
      }

      override def toString: String = {
        return "HdfsLock(lockFile=" + lockFile + ")"
      }
    }


  }
}
