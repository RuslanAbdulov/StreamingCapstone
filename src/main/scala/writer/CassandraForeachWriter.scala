package writer

import com.datastax.spark.connector.cql.CassandraConnector
import model.AggregateEvent
import org.apache.spark.SparkConf
import org.apache.spark.sql.ForeachWriter

class CassandraForeachWriter(val conf: SparkConf) extends ForeachWriter[AggregateEvent] {

  val namespace = "spotbot"
  val table = "bots"

  val column_ip = "ipAddress"
  val column_categoryId = "categoryId"
  val column_unixTime = "unixTime"
  val column_eventType = "eventType"
  val column_is_bot = "is_bot"

  val connector = CassandraConnector(conf)

  {
    connector.withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $namespace " +
        s"WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $namespace.$table " +
        s"(ipAddress text, categoryId text, unixTime text, eventType text, is_bot text, " +
        s" PRIMARY KEY (ipAddress, categoryId, unixTime, eventType))")
    }
  }

  override def open(partitionId: Long, epochId: Long): Boolean = true

  override def process(aggregateEvent: AggregateEvent): Unit = {
//    println(s"Process new $aggregateEvent")
    aggregateEvent.events.foreach(event => {
      connector.openSession()
        .executeAsync(
          s"""insert into $namespace.$table
              ($column_ip, $column_categoryId, $column_unixTime, $column_eventType, $column_is_bot )
         values (?, ?, ?, ?, ?)""",
          event.ipAddress, event.categoryId, event.unixTime, event.eventType, aggregateEvent.isBot.getOrElse(false).toString)
    })

  }

  override def close(errorOrNull: Throwable): Unit = Unit
}
