package roar.hbase.services

/**
  * split index thread
  *
  * 1. watch index split request folder in zookeeper
  * 2. split index
  * 3. notify region to load the new index
  * 4. remove parent directory and subdirectory
  * 5. complete the transaction
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-07-10
  */
class IndexSplitter {
}
