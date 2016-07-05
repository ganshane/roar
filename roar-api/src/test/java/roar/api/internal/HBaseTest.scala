package roar.api.internal

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test

/**
  * first hbase test case
  *
  * @author <a href="mailto:jcai@ganshane.com">Jun Tsai</a>
  * @since 2016-06-29
  */
class HBaseTest {
  @Test
  def test_hbase: Unit ={
    val table_name="test1"
    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "localhost");
    val conn = ConnectionFactory.createConnection(conf);

    val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    val listtables=admin.listTables()
    listtables.foreach(println)
    /*
    if (admin.tableExists(table_name))
    {
      admin.disableTable(table_name)
      admin.deleteTable(table_name)

    }
    val htd = new HTableDescriptor(table_name)
    val hcd = new HColumnDescriptor("id")
    //add  column to table
    htd.addFamily(hcd)
    admin.createTable(htd)
    */


    //put data to HBase table
//    val tablename = htd.getName
    val table = conn.getTable(TableName.valueOf(table_name))
    val databytes = Bytes.toBytes("id")
    for (c <- 1 to 10) {
      val row = Bytes.toBytes("row" + c.toString)
      val p1 = new Put(row)
      p1.addColumn(databytes, Bytes.toBytes(c.toString), Bytes.toBytes("value" + c.toString))
      table.put(p1)
    }
    for (c <- 1 to 10) {
      val g = new Get(Bytes.toBytes("row" + c.toString))
      println("Get:" + table.get(g))
    }

  }
}
