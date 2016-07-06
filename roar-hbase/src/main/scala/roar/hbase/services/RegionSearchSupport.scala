package roar.hbase.services

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.IOUtils
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, StringField}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}
import roar.hbase.RoarHbaseConstants
import stark.utils.services.LoggerSupport

/**
  * region search support
 *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
trait RegionSearchSupport {
  this:RegionIndexSupport with LoggerSupport with RegionCoprocessorEnvironmentSupport =>
  private var searcherManager:SearcherManager = _

  protected def openSearcherManager(): Unit = {
    searcherManager = new SearcherManager(indexWriter,new SearcherFactory())
  }
  def search(q:String,offset:Int=0,limit:Int=30): scala.List[Document]={
    var searcher: IndexSearcher = null
    try {
      searcher = searcherManager.acquire()
      println("max doc:"+searcher.getIndexReader.maxDoc())
      val parser = new QueryParser("id",RoarHbaseConstants.defaultAnalyzer)
      val query = parser.parse(q)
      val docs = searcher.search(query,offset+limit)
      if(docs.scoreDocs.length > offset) {
        docs.scoreDocs.drop(offset).map{scoreDoc =>
          convert(searcher.doc(scoreDoc.doc))
        }.toList
      } else Nil
    }finally {
      searcherManager.release(searcher)
    }
  }
  protected def mybeRefresh(): Unit ={
    searcherManager.maybeRefresh()
  }
  private def convert(d:Document):Document= {
    val timestampField = d.getField(RoarHbaseConstants.TIMESTAMP_FIELD)
    val timestamp = timestampField.numericValue().longValue()
    val rowField = d.getField(RoarHbaseConstants.ROW_FIELD)
    val rowBin= rowField.binaryValue()
    val row = new Array[Byte](rowBin.length)
    System.arraycopy(rowBin.bytes,rowBin.offset,row,0,row.length)
    val get = new Get(row)
    get.setTimeStamp(timestamp)
    
    val doc = new Document()
    val result = coprocessorEnv.getRegion.get(get, false)
    val size = result.size()

    Range(0,size).foreach{i=>
      val cell = result.get(i)
      val key = Bytes.toString(cell.getQualifierArray,cell.getQualifierOffset,cell.getQualifierLength)
      val value = Bytes.toString(cell.getValueArray,cell.getValueOffset,cell.getValueLength)
      //        System.out.println("keyStr:"+""+" keyStr2:"+keyStr2+" valStr2:"+valStr2)
      val field = new StringField(key, value, Store.NO)
      doc.add(field)
    }
    doc.add(rowField)
    doc
  }
  protected def closeSearcher(): Unit ={
    IOUtils.closeStream(searcherManager)
  }
}
