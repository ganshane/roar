package roar.hbase.internal

import java.util

import org.apache.lucene.index.{DocValues, LeafReaderContext, SortedDocValues}
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.util.{BytesRef, PriorityQueue, SentinelIntSet}

/**
  *
  * find group
  * 1. use TermAllGroupsCollector to find all group
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-09-08
  */
class FieldGroupCountCollector(field:String,groupNames:util.Collection[BytesRef]) extends SimpleCollector{
  private val orderSet = new SentinelIntSet(groupNames.size(),-2)
  private val groupMap = initGroupCountObject();
  private val groupCounts = new Array[GroupCount](orderSet.keys.length)
  private var index: SortedDocValues = _

  private var ord:Int = _
  private def initGroupCountObject():Map[BytesRef,GroupCount]={
    val it = groupNames.iterator()
    var map = Map[BytesRef,GroupCount]()
    while(it.hasNext){
      val name = it.next()
      map = map + (name->GroupCount(name))
    }

    map
  }
  override def collect(doc: Int): Unit = {
    ord = index.getOrd(doc)
    if(ord>=0){
      val slot = orderSet.find(ord)
      if(slot >=0)
        groupCounts(slot).count += 1
    }
  }

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    index = DocValues.getSorted(context.reader(),field)
    val it = groupNames.iterator()
    while(it.hasNext){
      val name = it.next()
      val ord = index.lookupTerm(name)
      groupCounts(orderSet.put(ord)) = groupMap.get(name).get
    }
  }
  override def needsScores(): Boolean = false

  def getTopGroups(topN:Int):Array[GroupCount]={
    val pq= new PriorityQueue[GroupCount](topN){
      override def lessThan(a: GroupCount, b: GroupCount): Boolean = {
        a.count <= b.count
      }
    }
    val it = groupMap.values.iterator
    while(it.hasNext)
      pq.insertWithOverflow(it.next())

    Range(0,pq.size()).map(i=>pq.pop()).toArray
  }
}
case class GroupCount(bytesRef: BytesRef){
  var count=0
}
