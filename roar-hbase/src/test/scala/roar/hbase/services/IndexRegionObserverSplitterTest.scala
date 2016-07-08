/**
  *
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package roar.hbase.services

import java.io.IOException
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.coprocessor.{BaseRegionObserver, CoprocessorHost, ObserverContext, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, LruBlockCache}
import org.apache.hadoop.hbase.regionserver._
import org.apache.hadoop.hbase.testclassification.{RegionServerTests, SmallTests}
import org.apache.hadoop.hbase.util.{Bytes, FSUtils, PairOfSameType}
import org.apache.hadoop.hbase.wal.WALFactory
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}
import org.junit.experimental.categories.Category
import org.mockito.Mockito
import org.mockito.Mockito.when
import roar.hbase.model.ResourceDefinition
import stark.utils.services.XmlLoader

/**
  * Test the {@link SplitTransactionImpl} class against an HRegion (as opposed to
  * running cluster).
  */
@Category(Array(classOf[RegionServerTests], classOf[SmallTests])) 
object IndexRegionObserverSplitterTest{
  private val STARTROW: Array[Byte] = Array[Byte]('a', 'a', 'a')
  private val ENDROW: Array[Byte] = Array[Byte]('{', '{', '{')
  private val GOOD_SPLIT_ROW: Array[Byte] = Array[Byte]('d', 'd', 'd')
  private val CF: Array[Byte] = Bytes.toBytes("info")
  private var preRollBackCalled: Boolean = false
  private var postRollBackCalled: Boolean = false

  class CustomObserver extends BaseRegionObserver {
    @throws(classOf[IOException])
    override def preRollBackSplit(ctx: ObserverContext[RegionCoprocessorEnvironment]) {
      preRollBackCalled = true
    }

    @throws(classOf[IOException])
    override def postRollBackSplit(ctx: ObserverContext[RegionCoprocessorEnvironment]) {
      postRollBackCalled = true
    }
  }

}

@Category(Array(classOf[RegionServerTests], classOf[SmallTests]))
class IndexRegionObserverSplitterTest{
  private final val TEST_UTIL: HBaseTestingUtility = new HBaseTestingUtility
  private final val testdir: Path = TEST_UTIL.getDataTestDir(this.getClass.getName)
  private var parent: HRegion = null
  private var wals: WALFactory = null
  private var fs: FileSystem = null
  private val row1 = Bytes.toBytes("r1")
  private val xm = Bytes.toBytes("xm")
  private val xb = Bytes.toBytes("xb")

  @Before
  @throws(classOf[IOException])
  def setup {
    val rd = XmlLoader.parseXML[ResourceDefinition](getClass.getResourceAsStream("/test_res.xml"), None)
    RegionServerData.regionServerResources = Map("czrk"->rd)

    this.fs = FileSystem.get(TEST_UTIL.getConfiguration)
    TEST_UTIL.getConfiguration.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, classOf[IndexRegionObserver].getName)
    this.fs.delete(this.testdir, true)
    val walConf: Configuration = new Configuration(TEST_UTIL.getConfiguration)
    FSUtils.setRootDir(walConf, this.testdir)
    this.wals = new WALFactory(walConf, null, this.getClass.getName)
    this.parent = createRegion(this.testdir, this.wals)
    val host: RegionCoprocessorHost = new RegionCoprocessorHost(this.parent, null, TEST_UTIL.getConfiguration)
    this.parent.setCoprocessorHost(host)
    TEST_UTIL.getConfiguration.setBoolean("hbase.testing.nocluster", true)
    host.postOpen
  }

  @After
  @throws(classOf[IOException])
  def teardown {
    if (this.parent != null && !this.parent.isClosed) this.parent.close
    val regionDir: Path = this.parent.getRegionFileSystem.getRegionDir
    if (this.fs.exists(regionDir) && !this.fs.delete(regionDir, true)) {
      throw new IOException("Failed delete of " + regionDir)
    }
    if (this.wals != null) {
      this.wals.close
    }
    this.fs.delete(this.testdir, true)
  }

  /**
    * Test straight prepare works.  Tries to split on {@link #GOOD_SPLIT_ROW}
    *
    * @throws IOException
    */
  @Test
  @throws(classOf[IOException])
  def testPrepare {
    prepareGOOD_SPLIT_ROW
  }

  @throws(classOf[IOException])
  private def prepareGOOD_SPLIT_ROW: SplitTransactionImpl = {
    return prepareGOOD_SPLIT_ROW(this.parent)
  }

  @throws(classOf[IOException])
  private def prepareGOOD_SPLIT_ROW(parentRegion: HRegion): SplitTransactionImpl = {
    val st: SplitTransactionImpl = new SplitTransactionImpl(parentRegion, IndexRegionObserverSplitterTest.GOOD_SPLIT_ROW)
    assertTrue(st.prepare)
    return st
  }

  @Test
  def testWholesomeSplit():Unit= {
    var rowcount: Int = TEST_UTIL.loadRegion(this.parent, IndexRegionObserverSplitterTest.CF, true)
    assertTrue(rowcount > 0)

      //add some data
      val p = new Put(row1)
      p.addColumn(IndexRegionObserverSplitterTest.CF, xm, xm)
      parent.put(p)
    rowcount += 1

    val parentRowCount: Int = countRows(this.parent)
    assertEquals(rowcount, parentRowCount)
    val cacheConf: CacheConfig = new CacheConfig(TEST_UTIL.getConfiguration)
    (cacheConf.getBlockCache.asInstanceOf[LruBlockCache]).clearCache
    val st: SplitTransactionImpl = prepareGOOD_SPLIT_ROW
    val mockServer: Server = Mockito.mock(classOf[Server])
    when(mockServer.getConfiguration).thenReturn(TEST_UTIL.getConfiguration)
    val daughters: PairOfSameType[Region] = st.execute(mockServer, null)
    assertTrue(this.parent.isClosed)
    assertTrue(Bytes.equals(parent.getRegionInfo.getStartKey, daughters.getFirst.getRegionInfo.getStartKey))
    assertTrue(Bytes.equals(IndexRegionObserverSplitterTest.GOOD_SPLIT_ROW, daughters.getFirst.getRegionInfo.getEndKey))
    assertTrue(Bytes.equals(daughters.getSecond.getRegionInfo.getStartKey, IndexRegionObserverSplitterTest.GOOD_SPLIT_ROW))
    assertTrue(Bytes.equals(parent.getRegionInfo.getEndKey, daughters.getSecond.getRegionInfo.getEndKey))
    var daughtersRowCount: Int = 0
    import scala.collection.JavaConversions._
    for (openRegion <- daughters) {
      try {
        val count: Int = countRows(openRegion)
        assertTrue(count > 0 && count != rowcount)
        daughtersRowCount += count
      } finally {
        HBaseTestingUtility.closeRegionAndWAL(openRegion)
      }
    }
    assertEquals(rowcount, daughtersRowCount)
  }

  private def wasRollBackHookCalled: Boolean = {
    return (IndexRegionObserverSplitterTest.preRollBackCalled && IndexRegionObserverSplitterTest.postRollBackCalled)
  }

  /**
    * Exception used in this class only.
    */
  @SuppressWarnings(Array("serial")) private class MockedFailedDaughterCreation extends IOException {
  }

  private class MockedFailedDaughterOpen extends IOException {
  }

  @throws(classOf[IOException])
  private def countRows(r: Region): Int = {
    var rowcount: Int = 0
    val scanner: InternalScanner = r.getScanner(new Scan)
    try {
      val kvs: util.List[Cell] = new util.ArrayList[Cell]
      var hasNext: Boolean = true
      while (hasNext) {
        hasNext = scanner.next(kvs)
        if (!kvs.isEmpty)
          rowcount += 1;
      }
    } finally {
      scanner.close
    }
    return rowcount
  }

  @throws(classOf[IOException])
  private[services] def createRegion(testdir: Path, wals: WALFactory): HRegion = {
    val htd: HTableDescriptor = new HTableDescriptor(TableName.valueOf("czrk"))
    val hcd: HColumnDescriptor = new HColumnDescriptor(IndexRegionObserverSplitterTest.CF)
    htd.addFamily(hcd)
    val hri: HRegionInfo = new HRegionInfo(htd.getTableName, IndexRegionObserverSplitterTest.STARTROW, IndexRegionObserverSplitterTest.ENDROW)
    val r: HRegion = HBaseTestingUtility.createRegionAndWAL(hri, testdir, TEST_UTIL.getConfiguration, htd)
    HBaseTestingUtility.closeRegionAndWAL(r)
    return HRegion.openHRegion(testdir, hri, htd, wals.getWAL(hri.getEncodedNameAsBytes), TEST_UTIL.getConfiguration)
  }
}
