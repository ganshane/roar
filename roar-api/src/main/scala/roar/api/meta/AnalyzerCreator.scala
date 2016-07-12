// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.api.meta

import org.apache.lucene.analysis.Analyzer

/**
 * 分词器的创建
 *
 * @author jcai
 */
object AnalyzerCreator {
  def create(analyzerType: AnalyzerType) = {
    if (analyzerType == null) {
      throw new IllegalArgumentException("analyzerType is null!")
    }

    val clazz = analyzerType.clazz
    clazz.newInstance().asInstanceOf[Analyzer]
  }
}
