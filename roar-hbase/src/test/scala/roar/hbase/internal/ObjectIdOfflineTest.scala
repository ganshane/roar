package roar.hbase.internal

import java.io.File
import java.util.Calendar

import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable
import scala.io.Source

/**
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-09-11
  */
object ObjectIdOfflineTest {
  private val sfzhPattern="([\\d]{6})([\\d]{4})([\\d]{2})([\\d]{2})([\\d]{3})([\\dXx])".r
  def main(args:Array[String]): Unit = {
    val data = mutable.Map[String, RoaringBitmap]()
    val stream = Source.fromFile(new File("/Users/jcai/Downloads/sfzh.txt"))
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(0)
    val maxTime = 1 << 16
    val minTime = (1970 - 1900) * 365
    stream.getLines()
//        .take(5)
      .foreach {
      case sfzhPattern(district, y, m, d, seq, _) =>
        calendar.set(Calendar.YEAR, y.toInt)
        calendar.set(Calendar.MONTH, m.toInt)
        calendar.set(Calendar.DAY_OF_MONTH, d.toInt)
        var days = (calendar.getTimeInMillis / 1000 / 60 / 60 / 24).toInt + minTime
        if (days > maxTime)
          throw new IllegalStateException("time is bigger " + maxTime)

//        println(y,m,d,days,days.toBinaryString)
      /*{
        days <<= 14
        days |= seq.toInt
      }
      */
        {
          days |= (seq.toInt << 16)
        }
//        println(days,seq,days.toBinaryString)

        data.get(district) match {
          case Some(bitmap) =>
            bitmap.add(days)
          case None =>
            val bitmap = new RoaringBitmap()
            data.put(district, bitmap)
            bitmap.add(days)
        }
      case other=>
          println("invalid sfzh "+other)
    }
    //371102198510147846
    val bytes = data.values.map(_.getSizeInBytes).sum
    val maxBytes = data.values.map(_.getSizeInBytes).max
    println(data.size,bytes,maxBytes)


    /*
    var i = 0
    data.values.foreach{b=>
      val file = new File("/tmp/monad/"+i)
      val fos = new FileOutputStream(file)
      b.serialize(new DataOutputStream(fos))
      fos.close()
      i += 1
    }
    */

    val start = System.currentTimeMillis()
    val stream2 = Source.fromFile(new File("/Users/jcai/Downloads/sfzh.txt"))
    stream2.getLines()
              .take(1000000)
      .foreach {
      case sfzhPattern(district, y, m, d, seq, _) =>
        calendar.set(Calendar.YEAR, y.toInt)
        calendar.set(Calendar.MONTH, m.toInt)
        calendar.set(Calendar.DAY_OF_MONTH, d.toInt)
        var days = (calendar.getTimeInMillis / 1000 / 60 / 60 / 24).toInt + minTime
        if (days > maxTime)
          throw new IllegalStateException("time is bigger " + maxTime)

        //        println(y,m,d,days,days.toBinaryString)
        /*{
          days <<= 14
          days |= seq.toInt
        }
        */
      {
        days |= (seq.toInt << 16)
      }
        //        println(days,seq,days.toBinaryString)

        data.get(district) match {
          case Some(bitmap) =>
            bitmap.contains(days)
          case None =>
        }
      case other=>
        println("invalid sfzh "+other)
    }
    println(System.currentTimeMillis() - start)

  }
}
