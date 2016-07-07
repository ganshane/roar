package roar.hbase.services

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.regionserver.wal.FSHLog
import org.apache.hadoop.hbase.regionserver.{Region, HRegion, RegionCoprocessorHost}
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.hbase.wal.DefaultWALProvider
import org.junit.{Assert, Test}
import roar.hbase.model.ResourceDefinition
import roar.protocol.generated.RoarProtos.{IndexSearchService, SearchRequest, SearchResponse}
import stark.utils.services.XmlLoader

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-07
  */
class IndexRegionObserverRestoreTest {
  private var TEST_UTIL:HBaseTestingUtility = _
  private val tableName = TableName.valueOf("czrk")
  private val family = Bytes.toBytes("info")
  private val ee = EnvironmentEdgeManager.getDelegate();

  private val row1 = Bytes.toBytes("r1")
  private val xm = Bytes.toBytes("xm")
  private val xb = Bytes.toBytes("xb")
  private var region:HRegion = _
  private var conf:Configuration = _


  @Test
  def test_wal: Unit ={
    conf = HBaseConfiguration.create()
    TEST_UTIL = new HBaseTestingUtility(conf)
    TEST_UTIL.startMiniCluster();

    val rd = XmlLoader.parseXML[ResourceDefinition](getClass.getResourceAsStream("/test_res.xml"), None)
    RegionServerData.regionServerResources = Map("czrk"->rd)

    val tableDesc = new HTableDescriptor(tableName)
    val colFamilyDesc = new HColumnDescriptor(family)
    tableDesc.addFamily(colFamilyDesc)
//    tableDesc.addCoprocessor(classOf[IndexRegionObserver].getName)

    val hbaseRootDir = FSUtils.getRootDir(conf);
    val regionInfo = new HRegionInfo(tableName, null, null, false);
    region = HBaseTestingUtility.createRegionAndWAL(regionInfo, hbaseRootDir, conf, tableDesc);
    HBaseTestingUtility.closeRegionAndWAL(region);


    val fs = TEST_UTIL.getDFSCluster().getFileSystem();
    val oldLogDir = new Path(hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    val logName = DefaultWALProvider.getWALDirectoryName("restore-manual");
    val logDir = new Path(hbaseRootDir, "asdf");
    if (TEST_UTIL.getDFSCluster().getFileSystem().exists(hbaseRootDir)) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(hbaseRootDir, true);
    }

    val wal = createWAL(this.conf, hbaseRootDir, logName);
    region = HRegion.openHRegion(conf, fs, hbaseRootDir, regionInfo, tableDesc, wal);


    val coprocessorHost = new RegionCoprocessorHost(region, null, conf)
    region.setCoprocessorHost(coprocessorHost)
    region.getCoprocessorHost.load(classOf[IndexRegionObserver],1073741823,conf)
    coprocessorHost.postOpen()


    addRegionEdits(row1, family, 10, this.ee, region, "xm");
    Assert.assertEquals(1,query("xm:9").getCount)


    region.close(true);
    wal.shutdown();


    TEST_UTIL.shutdownMiniCluster()

  }
  private def createWAL(c:Configuration,hbaseRootDir:Path,logName:String)={
    val wal = new FSHLog(FileSystem.get(c), hbaseRootDir, logName, c);
//    HBaseTestingUtility.setMaxRecoveryErrorCount(wal.getOutputStream(), 1);
    wal;
  }
  private def addRegionEdits(rowName:Array[Byte],
                             family:Array[Byte],
                             count:Int, ee:EnvironmentEdge ,
                             r:Region, qualifierPrefix:String):util.List[Put]= {
    val puts = new util.ArrayList[Put]();
    Range(0,count).foreach{j=>
      val p = new Put(rowName);
      p.addColumn(family, Bytes.toBytes(qualifierPrefix), ee.currentTime(), Bytes.toBytes(j.toString))
      r.put(p);
      puts.add(p);
    }
    return puts;
  }
  private def query(q:String):SearchResponse={
    val request = SearchRequest.newBuilder()
    request.setQ(q)
    val method = IndexSearchService.getDescriptor.findMethodByName("query")
    val call = ClientProtos.CoprocessorServiceCall.newBuilder
      .setRow(ByteStringer.wrap(HConstants.EMPTY_BYTE_ARRAY))
      .setServiceName(method.getService.getFullName)
      .setMethodName(method.getName)
      .setRequest(request.build().toByteString).build

    region.execService(null,call).asInstanceOf[SearchResponse]
  }
}
