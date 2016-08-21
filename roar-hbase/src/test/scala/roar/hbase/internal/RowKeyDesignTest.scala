package roar.hbase.internal

import org.apache.commons.codec.digest.DigestUtils
import org.junit.Test

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
  }
}
