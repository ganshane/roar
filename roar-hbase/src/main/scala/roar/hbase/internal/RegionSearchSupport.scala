package roar.hbase.internal

import com.google.protobuf.ByteString
import org.apache.hadoop.io.IOUtils
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search._
import roar.api.meta.ColumnType
import roar.api.meta.ResourceDefinition.ResourceProperty
import roar.hbase.services.{RegionCoprocessorEnvironmentSupport, RegionIndexSupport}
import roar.protocol.generated.RoarProtos.SearchResponse
import stark.utils.services.LoggerSupport

import scala.annotation.tailrec

/**
  * region search support
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-06
  */
trait RegionSearchSupport
  extends QueryParserSupport
    with SearcherManagerSupport
    with GroupCountSearcherSupport
    with ObjectIdSearcherSupport {
  this:RegionIndexSupport with RegionCoprocessorEnvironmentSupport with LoggerSupport =>
  private var searcherManagerOpt:Option[SearcherManager] = None


  //全局搜索对象
  override protected def getSearcherManager: Option[SearcherManager] = searcherManagerOpt

  private[hbase] def openSearcherManager(): Unit = {
    searcherManagerOpt = indexWriterOpt.map(new SearcherManager(_,new SearcherFactory(){

      initQueryParser(rd)

      override def newSearcher(reader: IndexReader, previousReader: IndexReader): IndexSearcher = {
        new InternalIndexSearcher(reader,rd,null)
      }
    }))
  }
  def numDoc: Int ={
    doInSearcher { s =>
      s.getIndexReader.numDocs()
    }.getOrElse(0)
  }
  def search(q:String,sortStringOpt: Option[String]=None, topN: Int=30): Option[SearchResponse] ={
    doInSearcher { searcher =>
      logger.info("[{}] \"{}\" sort:\"{}\" searching .... ", Array[AnyRef](rd.name, q, sortStringOpt))
      val query = parseQuery(q)
      //sort
      var sortOpt: Option[Sort] = None

      sortStringOpt match {
        case Some(sortStr) =>
          val it = queryResource.properties.iterator()
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
  private[hbase] def maybeRefresh(): Unit ={
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
  private[hbase] def closeSearcher(): Unit ={
    searcherManagerOpt.foreach(IOUtils.closeStream)
  }
}
