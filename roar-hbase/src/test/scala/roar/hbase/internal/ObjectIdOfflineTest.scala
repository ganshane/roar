package roar.hbase.internal

import java.io._

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
  private val y1900 = rdn(1900,1,1)

  /**
    * Convert your dates to integer denoting the number of days since an epoch, then subtract.
    * using Rata Die, an explanation of the algorithm can be found at <http://mysite.verizon.net/aesir_research/date/rata.htm>.
    *
    * @param year
    * @param month
    * @param d
    * @return
    */
  private def rdn(year:Int,month:Int,d:Int): Int ={
    var y = year
    var m = month
    if (m < 3) {
      y -= 1
      m += 12
    }
    365*y + y/4 - y/100 + y/400 + (153*m - 457)/5 + d - 306
  }
  private def calIdSeq(year:Int,month:Int,d:Int,seq:Int):Int={
    val days = rdn(year,month,d)
    (days - y1900) | (seq.toInt << 16)
    /*
    val days = (year - 1900) * 366 + (month-1) * 31 + d
    days | (seq << 16)
    */
  }
  def main(args:Array[String]): Unit = {
    if(calIdSeq(2010,12,16,736) !=48275022){
      throw new IllegalArgumentException
    }

    val data = mutable.Map[String, RoaringBitmap]()
    val stream = Source.fromFile(new File("/Users/jcai/Downloads/sfzh.txt"))
//    val minTime = (1970 - 1900) * 365
    var line = 0
    val mask = (1 << 20) -1
    stream.getLines()
//        .take(100000000)
      .foreach { l => l match {
      case sfzhPattern(district, y, m, d, seq, _) =>
        val days = calIdSeq(y.toInt,m.toInt,d.toInt,seq.toInt)
        //        println(days,seq,days.toBinaryString)
        line += 1
        data.get(district) match {
          case Some(bitmap) =>
//            println(l,district,days)
            bitmap.add(days)
            if((line & mask) == 0)
              println(line)
          case None =>
            val bitmap = new RoaringBitmap()
            data.put(district, bitmap)
            bitmap.add(days)
        }
      case other =>
        println("invalid sfzh " + other)
    }
    }


    data.foreach{case (k,v)=>
      val file = new File("/Users/jcai/workspace/tmp/monad-1/"+k)
      val fos = new FileOutputStream(file)
      if(v.runOptimize()){
        println(k+"-> "+v.serializedSizeInBytes()+" "+v.getCardinality)
      }
//      fos.write(ByteBuffer.allocate(4).putInt(k.toInt).array())
      v.serialize(new DataOutputStream(fos))
      fos.close()
    }
    //371102198510147846
    val bytes = data.values.map(_.getSizeInBytes).sum
    val maxBytes = data.values.map(_.getSizeInBytes).max
    println(data.size,bytes,maxBytes,"line:",line)

    val start = System.currentTimeMillis()
    var trueInt = 0
    var falseInt = 0
    val stream2 = Source.fromFile(new File("/Users/jcai/Downloads/sfzh-1.txt"))
    stream2.getLines()
              .take(100000000)
      .foreach {
      case sfzhPattern(district, y, m, d, seq, _) =>
        val days = calIdSeq(y.toInt,m.toInt,d.toInt,seq.toInt)

        data.get(district) match {
          case Some(bitmap) =>
            if(bitmap.contains(days)) trueInt += 1
            else falseInt += 1
          case None =>
            falseInt += 1
        }
      case other=>
        println("invalid sfzh "+other)
    }
    println(System.currentTimeMillis() - start,trueInt,falseInt)

  }
}
