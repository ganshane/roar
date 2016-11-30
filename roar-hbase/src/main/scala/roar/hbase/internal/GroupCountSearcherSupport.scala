// Copyright 2015 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.internal

import roar.hbase.services.RegionCoprocessorEnvironmentSupport
import stark.utils.services.LoggerSupport

/**
  * 对象搜索的
 *
 * @author jcai
 */
trait GroupCountSearcherSupport {
  this: SearcherManagerSupport
    with QueryParserSupport
    with RegionCoprocessorEnvironmentSupport
    with LoggerSupport =>
  /**
   * 搜索对象
 *
   * @param q 搜索条件
   * @return
   */
  def searchFreq(q: String,field:String,maxGroup:Int=10000,topN:Int=1000): Option[(Array[GroupCount],Boolean,Int)]= {
    doInSearcher { search =>
      val parser = createParser()
      val query = parser.parse(q)
      logger.info("object id query :{} ....", q)
      val start = System.currentTimeMillis()


      val groupsCollector = new TermAllGroupsCollector(field, maxGroup)
      try {
        search.search(query, groupsCollector)
      }catch{
        case e:IllegalStateException =>
      }
      val groups = groupsCollector.getGroups()
      val time1 = System.currentTimeMillis() - start
      logger.info("freq query step1 :{},size:{} time:" + time1, q, groups.size())
      val fieldCountCollector = new FieldGroupCountCollector(field,groups)
      search.search(query, fieldCountCollector)
      val result = fieldCountCollector.getTopGroups(topN)
      //originCollector.result.optimize()
      val time = System.currentTimeMillis() - start
      //      val resultSize = originCollector.result.cardinality()
      logger.info("freq query :{},size:{} time:" + time, q, result.length)

      (result,groupsCollector.isPartial,fieldCountCollector.totalHits)
    }
  }
}
