// Copyright 2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.hbase.services

import stark.utils.services.ErrorCode

/**
  * roar hbase exception code
 * @author jcai
 */
object RoarHbaseExceptionCode {

  case object ANALYZER_TYPE_IS_NULL extends ErrorCode(3001)

  case object RESOURCE_NOT_FOUND extends ErrorCode(3002)

  case object OBJECT_NOT_LIVE extends ErrorCode(3003)

  case object FAIL_GET_SELF_GROUP_CONFIG extends ErrorCode(3004)

  case object FAIL_CONNECT_GROUP_SERVER extends ErrorCode(3005)

  case object INDEX_TYPE_NOT_SUPPORTED extends ErrorCode(3006)

  case object RESOURCE_SEARCH_NO_FOUND extends ErrorCode(5001)

  case object QUERY_TIMEOUT extends ErrorCode(5002)

  case object HIGH_CONCURRENT extends ErrorCode(5003)

  case object SEARCHER_CLOSING extends ErrorCode(5004)

  case object UNABLE_UNMAP_BUFFER extends ErrorCode(5005)

  case object INVALID_INDEX_PAYLOAD extends ErrorCode(5006)

  case object OVERFLOW_DIRECT_BUFFER extends ErrorCode(5007)

  case object FAIL_TO_ALLOCATE_MEMORY extends ErrorCode(5008)

  case object FAIL_TO_LOCK_MEMORY extends ErrorCode(5009)

  case object NOSQL_LOG_DATA_IS_NULL extends ErrorCode(5010)

  case object INDEXER_WILL_SHUTDOWN extends ErrorCode(5011)

  case object FAIL_TO_PARSE_QUERY extends ErrorCode(5012)

  case object OBJECT_ID_IS_NULL extends ErrorCode(5013)

  case object OVERFLOW_RESOURCE_RANGE extends ErrorCode(5014)

  case object INVALID_BINLOG_SEQ extends ErrorCode(5015)


}
