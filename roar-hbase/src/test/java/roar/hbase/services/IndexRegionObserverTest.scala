package roar.hbase.services

import java.io.IOException

import junit.framework.Assert._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.util.Bytes
import org.junit.{Assert, After, Before, Test}
import roar.protocol.generated.RoarProtos.{SearchRequest, IndexSearchService}

/**
  * first hbase test case
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-06-29
  */
class IndexRegionObserverTest{
  private var util:HBaseTestingUtility = _
  private var htd:HTableDescriptor = _
  private var r:HRegion = _
  private val tableName = TableName.valueOf("test")
  private val dummy = Bytes.toBytes("dummy")
  private val row1 = Bytes.toBytes("r1")
  private val row2 = Bytes.toBytes("r2")
  private val row3 = Bytes.toBytes("r3")
  private val test = Bytes.toBytes("test")

  @Before
  def setup: Unit ={
    /*
    val baseDir = Files.createTempDirectory("test_hdfs_index").toFile.getAbsoluteFile
    val hadoopConf= new Configuration()
    hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(hadoopConf)
    val hdfsCluster = builder.build()
    val hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/"
    */


    val conf = HBaseConfiguration.create()
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      classOf[IndexRegionObserver].getName,classOf[IndexSearchEndpoint].getName)
//    conf.setStrings(RoarHbaseConstants.ROAR_INDEX_HDFS_CONF_KEY,hdfsURI)

    util = new HBaseTestingUtility(conf)
    util.startMiniCluster()


    val admin = util.getHBaseAdmin()
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName)
      }
      admin.deleteTable(tableName)
    }

    util.createTable(tableName, Array[Array[Byte]](dummy, test))



  }
  @After
  def tearDown: Unit ={
    util.shutdownMiniCluster()
  }

  @Test
  def testSimple: Unit ={
    /*
    val t = util.getConnection.getTable(tableName)
    val p = new Put(row1)
    p.addColumn(test,dummy,dummy)
    t.put(p)
    checkRowAndDelete(t,row1,1)
    t.close()
    */

    val t: Table = util.getConnection.getTable(tableName)
    var p: Put = new Put(row1)
    p.addColumn(test, dummy, dummy)
    // before HBASE-4331, this would throw an exception
    t.put(p)

    val channel = t.coprocessorService(row1)
    val service = IndexSearchService.newBlockingStub(channel)
    val request = SearchRequest.newBuilder()
    request.setQ("dummy:dummy")
    val response = service.query(null,request.build())
    Assert.assertEquals(1,response.getCount)

    checkRowAndDelete(t, row1, 1)
    p = new Put(row1)
    p.addColumn(test, dummy, dummy)
    // before HBASE-4331, this would throw an exception
    t.put(p)
    checkRowAndDelete(t, row1, 1)
    t.close
  }

  @throws(classOf[IOException])
  private def checkRowAndDelete(t: Table, row: Array[Byte], count: Int) {
    val g: Get = new Get(row)
    val r: Result = t.get(g)
    assertEquals(count, r.size)
    val d: Delete = new Delete(row)
    t.delete(d)
  }
  /*
  private def checkRowAndDelete(t:Table , row:Array[Byte], count:Int){
    val g = new Get(row)
    val r = t.get(g)
    Assert.assertEquals(count, r.size())
    val d = new Delete(row)
    t.delete(d)
  }
  */
}
