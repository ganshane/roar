package roar.hbase.internal

import org.apache.lucene.util.SentinelIntSet
import org.junit.Test

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-09-08
  */
class OrderSetTest {
  @Test
  def test_orderSet: Unit ={
    val ordSet = new SentinelIntSet(512, -2);
    println(ordSet.keys.length)
    Range(1000,2000).foreach{i=>
      println(i,ordSet.put(i))
    }
  }

}
