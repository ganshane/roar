package roar.hbase.internal

import org.apache.commons.codec.digest.DigestUtils
import org.apache.lucene.util.{RoaringDocIdSet, SparseFixedBitSet}
import org.junit.Test
import org.roaringbitmap.RoaringBitmap

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-08-21
  */
class RowKeyDesignTest {
  @Test
  def test_row: Unit ={
    val id = "413029198009121514"
    val bytes = DigestUtils.md5(id)
    println(bytes.length)
//    Range(1,100000).foreach(builder.addOrd)

    val bitSet = new SparseFixedBitSet(1000000)
    Range(1,10000,10).foreach(bitSet.set)
    println(bitSet.ramBytesUsed())
    val builder = new RoaringDocIdSet.Builder(1000000)
    Range(1,10000,10).foreach(builder.add)
    val rd = builder.build()
    println(rd.ramBytesUsed())
    val rr = new RoaringBitmap();
    Range(1,10000,10).foreach(rr.add)
    println(rr.getSizeInBytes)
    val rr2 = new RoaringBitmap();
    Range(1,10000,8).foreach(rr2.add)
    println(rr2.getSizeInBytes)
    rr2.and(rr)
    println(rr2.getSizeInBytes)

  }
}
