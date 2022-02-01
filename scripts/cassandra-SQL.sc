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

// affichage de la table
req1.show()

// creation de la nouvelle table
req1.createCassandraTable("test", "question1", partitionKeyColumns = Some(Seq("date")), clusteringKeyColumns = Some(Seq("actiongeocountrycode")))

// insertion des valeurs dans la nouvelle table
req1.write.cassandraFormat("question1", "test", "").mode("append").save()

// alternative avec "overwrite"
//req1.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(keyspace="test",table="question1").save()
