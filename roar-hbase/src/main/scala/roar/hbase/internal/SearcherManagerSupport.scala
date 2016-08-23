// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.internal

import java.util.concurrent.{Semaphore, TimeUnit}

import org.apache.lucene.search.SearcherManager
import org.apache.lucene.store.AlreadyClosedException
import roar.hbase.services.RoarHbaseExceptionCode
import stark.utils.services.StarkException

/**
 * 支持搜索管理
 *
 * @author jcai
 */
trait SearcherManagerSupport {
  private val semaphore = new Semaphore(5)

  //全局搜索对象
  protected def getSearcherManager: Option[SearcherManager]

  private[roar] def doInSearcher[T](fun: InternalIndexSearcher => T): Option[T] = {
    getSearcherManager map { sm =>
      if (semaphore.tryAcquire(60, TimeUnit.SECONDS)) {
        try {
          val s = sm.acquire().asInstanceOf[InternalIndexSearcher]
          try {
            fun(s)
          } finally {
            sm.release(s)
          }
        } catch {
          case e: AlreadyClosedException =>
            throw new StarkException("Server正在关闭", RoarHbaseExceptionCode.SEARCHER_CLOSING)
        } finally {
          semaphore.release()
        }
      } else {
        throw new StarkException("high load,timeout to acquire searcher",
          RoarHbaseExceptionCode.HIGH_CONCURRENT)
      }
    }
  }
}
