package cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.Implicits._
import com.websudos.phantom.column.{CounterColumn, DateTimeColumn, PrimitiveColumn}
import com.websudos.phantom.keys.PartitionKey
import com.websudos.phantom.testing.CassandraFlatSpec
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._

import scala.concurrent.{Await, ExecutionContext}

case class TestCassandraCounter(entityType: String, entityId: Long, startsAt: DateTime, actionId: String, value: Long)

sealed class TestCassandraCounterTable() extends CassandraTable[TestCassandraCounterTable, TestCassandraCounter] {

  override def tableName: String = "batch_test_table"

  object entity_type extends StringColumn(this) with PartitionKey[String]

  object entity_id extends LongColumn(this) with PartitionKey[Long]

  object action_id extends StringColumn(this) with PartitionKey[String]

  object time_bucket_starts_at extends DateTimeColumn(this) with PrimaryKey[DateTime] {
    override val isClusteringKey = true
  }

  object value extends CounterColumn(this)

  override def fromRow(r: Row): TestCassandraCounter = {
    TestCassandraCounter(
      entity_type(r),
      entity_id(r),
      time_bucket_starts_at(r),
      action_id(r),
      value(r)
    )
  }
}

class ShowBatchProblem extends CassandraFlatSpec with ScalaFutures with BeforeAndAfterEach {
  override def keySpace: String = "batch_test_keyspace"

  implicit val executionContext = ExecutionContext.global

  // Disable embedded Cassandra which has conflicting Thrift Server version
  // Requires Cassandra on localhost
  override def setupCassandra(): Unit = {}

  override protected def beforeEach() {
    import scala.util.control.Exception._
    allCatch.opt(session.execute(s"drop keyspace $keySpace;"))
    session.execute(s"create KEYSPACE $keySpace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")

    session.execute(
      """
        |CREATE TABLE batch_test_table (
        |  entity_type text,
        |  entity_id bigint,
        |  action_id text,
        |  time_bucket_starts_at timestamp,
        |  value counter,
        |  PRIMARY KEY ((entity_type, entity_id, action_id), time_bucket_starts_at)
        |) WITH CLUSTERING ORDER BY (time_bucket_starts_at DESC) AND
        |  bloom_filter_fp_chance=0.010000 AND
        |  caching='KEYS_ONLY' AND
        |  comment='' AND
        |  dclocal_read_repair_chance=0.100000 AND
        |  gc_grace_seconds=864000 AND
        |  index_interval=128 AND
        |  read_repair_chance=0.000000 AND
        |  replicate_on_write='true' AND
        |  populate_io_cache_on_flush='false' AND
        |  default_time_to_live=0 AND
        |  speculative_retry='99.0PERCENTILE' AND
        |  memtable_flush_period_in_ms=0 AND
        |  compaction={'class': 'SizeTieredCompactionStrategy'} AND
        |  compression={'sstable_compression': 'LZ4Compressor'};
      """.stripMargin)
  }

  "Cassandra" should "large handle batches" in {
    val table = new TestCassandraCounterTable
    val batch = CounterBatchStatement()
    val batchSize = 30000 // This fails with values > 21845
    (0 until batchSize).foreach(_ =>
      batch.add(
        table.update
          .where(_.entity_type eqs "foo")
          .and(_.entity_id eqs 1)
          .and(_.action_id eqs "bar")
          .and(_.time_bucket_starts_at eqs new DateTime())
          .modify(_.value increment 1)
      )
    )
    Await.result(batch.future, 10 seconds)
  }
}
