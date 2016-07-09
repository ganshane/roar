package roar.api.services

import com.google.protobuf.ByteString
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.{Assert, Test}
import roar.protocol.generated.RoarProtos.SearchResponse

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-09
  */
class RoarClientTest {
  @Test
  def testMerge: Unit ={
    val conf = HBaseConfiguration.create()
    val client = new RoarClient(conf)

    val response1 = SearchResponse.newBuilder()
    response1.setCount(1000)
    response1.setTotal(2000)
    response1.setMaxScore(100)
    Range(0,10).foreach{i=>
      val rowBuilder = response1.addRowBuilder(i)
      rowBuilder.setRowId(ByteString.copyFromUtf8("row1_"+i))
      val score = if(i ==0) 100 else (10 - i)*2
      rowBuilder.setScore(score)
    }
    val response2 = SearchResponse.newBuilder();
    {
      response2.setCount(900)
      response2.setTotal(2000)
      response2.setMaxScore(200)
      Range(0, 10).foreach { i =>
        val rowBuilder = response2.addRowBuilder(i)
        rowBuilder.setRowId(ByteString.copyFromUtf8("row2_" + i))
        val score = if (i == 0) 200 else (10-i) * 3
        rowBuilder.setScore(score)
      }
    }

    val list =List(response1.build(),response2.build())
    val response = client.mergeAux(0,10,list)

    Assert.assertEquals(1000+900,response.getCount)
    Assert.assertEquals(response2.getMaxScore,response.getMaxScore,0)
    Assert.assertEquals(10,response.getRowCount)
    Assert.assertEquals(response2.getRow(0),response.getRow(0))
    Assert.assertEquals(response1.getRow(3),response.getRow(9))

  }
}
