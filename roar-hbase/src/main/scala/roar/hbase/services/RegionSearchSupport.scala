package roar.hbase.services

import com.google.protobuf.ByteString
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.io.IOUtils
import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search._
import org.apache.lucene.util.BytesRef
import roar.api.meta.ResourceDefinition
import roar.hbase.internal.{InternalIndexSearcher, QueryParserSupport, SearcherManagerSupport}
import roar.api.meta.ColumnType
import ResourceDefinition.ResourceProperty
import roar.protocol.generated.RoarProtos.SearchResponse
import stark.utils.services.LoggerSupport

import scala.annotation.tailrec

/**
  * region search support
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
trait RegionSearchSupport extends QueryParserSupport with SearcherManagerSupport{
  this:RegionIndexSupport with LoggerSupport with RegionCoprocessorEnvironmentSupport =>
  private var searcherManagerOpt:Option[SearcherManager] = None


  //全局搜索对象
  override protected def getSearcherManager: SearcherManager = searcherManagerOpt.get

  protected def openSearcherManager(): Unit = {
    searcherManagerOpt = indexWriterOpt.map(new SearcherManager(_,new SearcherFactory(){

      initQueryParser(rd)

      override def newSearcher(reader: IndexReader, previousReader: IndexReader): IndexSearcher = {
        new InternalIndexSearcher(reader,rd,null)
      }
    }))
  }
  def search(q:String,sortStringOpt: Option[String]=None, topN: Int=30): Option[SearchResponse] ={
    searcherManagerOpt map { searcherManager =>
      logger.info("[{}] \"{}\" sort:\"{}\" searching .... ", Array[AnyRef](rd.name, q, sortStringOpt))
      val query = parseQuery(q)
      //sort
      var sortOpt: Option[Sort] = None

      sortStringOpt match{
        case Some(sortStr)=>
        val it = rd.properties.iterator()
        sortOpt = sortStr.trim.split("\\s+").toList match {
          case field :: "asc" :: Nil =>
            createSortField(field, false, it)
          case field :: Nil =>
            createSortField(field, false, it)
          case field :: "desc" :: Nil =>
            createSortField(field, true, it)
          case o =>
            None
        }
        case _ =>
      }

      doInSearcher { searcher =>
        val startTime = System.currentTimeMillis()
        val booleanQuery = new BooleanQuery.Builder
        booleanQuery.add(query, BooleanClause.Occur.MUST)

        val topDocs = sortOpt match {
          case Some(sort) =>
            searcher.search(booleanQuery.build(), topN, sort)
          case None =>
            searcher.search(booleanQuery.build(), topN)
        }

        //searcher.search(query, filter,collector)
        //val topDocs = collector.topDocs(topN)
        //val topDocs = searcher.search(query, filter, topN,sort)
        val endTime = System.currentTimeMillis()
        logger.info("[{}] q:{},time:{}ms,hits:{}",
          Array[Object](rd.name, q,
            (endTime - startTime).asInstanceOf[Object],
            topDocs.totalHits.asInstanceOf[Object]))
        val responseBuilder = SearchResponse.newBuilder()
        responseBuilder.setCount(topDocs.totalHits)
        responseBuilder.setTotal(searcher.getIndexReader.numDocs())
        responseBuilder.setMaxScore(topDocs.getMaxScore)

        topDocs.scoreDocs.foreach { scoreDoc =>
          val rowBuilder = responseBuilder.addRowBuilder()
          val rowId = searcher.rowId(scoreDoc.doc)
          rowBuilder.setRowId(ByteString.copyFrom(rowId.bytes, rowId.offset, rowId.length))
          rowBuilder.setScore(scoreDoc.score)
        }

        responseBuilder.build()
      }
    }

  }
  protected def mybeRefresh(): Unit ={
    searcherManagerOpt.foreach(_.maybeRefresh)
  }
  @tailrec
  private def createSortField(sort:String,reverse:Boolean,it:java.util.Iterator[ResourceProperty]): Option[Sort] ={
    if(it.hasNext){
      val property = it.next()
      if(property.name == sort){
        property.columnType match{
          case ColumnType.Long | ColumnType.Date =>
            Some(new Sort(new SortField(sort,SortField.Type.LONG,reverse)))
          case ColumnType.Int =>
            Some(new Sort(new SortField(sort,SortField.Type.INT,reverse)))
          case other=>
            logger.error("{} sort unsupported ",sort)
            None
        }
      }else{
        createSortField(sort,reverse,it)
      }
    }else{
      None
    }
  }

  private def convert(rowBin:BytesRef):Document= {
    val row = new Array[Byte](rowBin.length)
    System.arraycopy(rowBin.bytes,rowBin.offset,row,0,row.length)
    val get = new Get(row)
//    get.setTimeStamp(timestamp)
    
    val doc = new Document()
    val result = coprocessorEnv.getRegion.get(get, false)
    val size = result.size()
    /*

    Range(0,size).foreach{i=>
      val cell = result.get(i)
      val key = Bytes.toString(cell.getQualifierArray,cell.getQualifierOffset,cell.getQualifierLength)
      val value = Bytes.toString(cell.getValueArray,cell.getValueOffset,cell.getValueLength)
      //        System.out.println("keyStr:"+""+" keyStr2:"+keyStr2+" valStr2:"+valStr2)
      val field = new StringField(key, value, Store.NO)
      doc.add(field)
    }
    */
//    doc.add(rowField)
    doc
  }
  protected def closeSearcher(): Unit ={
    searcherManagerOpt.foreach(IOUtils.closeStream)
  }
}
