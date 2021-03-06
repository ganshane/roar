package roar.api

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Calendar

/**
  * some util methods
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-12
  */
package object meta {
  private val V2012 = Calendar.getInstance()
  V2012.setTimeInMillis(0)
  V2012.set(Calendar.YEAR,2012)
  V2012.set(Calendar.MONTH,1)
  V2012.set(Calendar.DAY_OF_MONTH,1)
  private val V2012_MILLIS = V2012.getTimeInMillis
  private val HOUR_IN_MILLIS= 1000 * 60 * 60
  private val V1980 = Calendar.getInstance()
  V1980.setTimeInMillis(0)
  V1980.set(Calendar.YEAR,1980)
  V1980.set(Calendar.MONTH,1)
  V1980.set(Calendar.DAY_OF_MONTH,1)
  private val V1980InMillis = V1980.getTimeInMillis
  private val ONE_MINUTE_IN_MILLIS= 60L * 1000
  def convertDateAsInt(millis:Long):Int={
    ((millis - V1980InMillis) / ONE_MINUTE_IN_MILLIS).toInt
  }
  def convertIntAsDate(minutesDiff:Int):Long ={
    (V1980InMillis + minutesDiff * ONE_MINUTE_IN_MILLIS)
  }
  /*
  def getNowHour:Int = {
      ((System.currentTimeMillis() - V2012_MILLIS) /  HOUR_IN_MILLIS).toInt
  }
  def dateToInt(time:Long):Int = {
      ((time - V2012_MILLIS) / HOUR_IN_MILLIS).toInt
  }
  */
  def convertAsArray(long:Long)={
    ByteBuffer.allocate(java.lang.Long.SIZE/8).order(ByteOrder.BIG_ENDIAN).putLong(long).array()
  }
  def convertIntAsArray(i:Int):Array[Byte]={
    ByteBuffer.allocate(4).putInt(i).array()
  }
  def convertAsShort(arr:Array[Byte],offset:Int=0):Short={
    ByteBuffer.wrap(arr,offset,2).getShort
  }
  def convertAsInt(arr:Array[Byte],offset:Int=0):Int={
    ByteBuffer.wrap(arr,offset,4).getInt
  }
  def convertAsLong(arr:Array[Byte],offset:Int=0):Long={
    ByteBuffer.wrap(arr,offset,8).getLong
  }
  def convertAsLong(arr:Option[Array[Byte]]):Option[Long]={
    if (arr.isDefined){
      Some(ByteBuffer.wrap(arr.get).getLong)
    }else{
      None
    }
  }

}
