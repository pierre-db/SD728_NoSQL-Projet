import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

val event = spark.read.cassandraFormat("event", "test").load()
val mentions = spark.read.cassandraFormat("mentions", "test").load()
val kb = spark.read.cassandraFormat("kb", "test").load()

event.createOrReplaceTempView("event")
mentions.createOrReplaceTempView("mentions")
kb.createOrReplaceTempView("kb")

val req1 = spark.sql("SELECT event.date, mentions.language, event.actiongeocountrycode, COUNT(*) as count FROM event, mentions WHERE event.globaleventid = mentions.globaleventid GROUP BY event.date, mentions.language, event.actiongeocountrycode ORDER BY count DESC;")

req1.show()