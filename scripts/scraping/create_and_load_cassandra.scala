import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

// chargement des fichiers dans des DF
// val event = spark.read.cassandraFormat("event", "test").load()
val event = spark.read.format("csv").options(Map("header"->"false", "delimiter" -> "\t", "encoding" -> "ISO-8859-1", "inferSchema" -> "true")).load("*.export.CSV").cache
val mentions = spark.read.format("csv").options(Map("header"->"false", "delimiter" -> "\t", "encoding" -> "ISO-8859-1", "inferSchema" -> "true")).load("*.mentions.CSV").cache
val gkg = spark.read.format("csv").options(Map("header"->"false", "delimiter" -> "\t", "encoding" -> "ISO-8859-1", "inferSchema" -> "true")).load("*.gkg.csv").cache

// créationde vues pour interragir en SQL
event.createOrReplaceTempView("event")
mentions.createOrReplaceTempView("mentions")
kb.createOrReplaceTempView("kb")

// définition de la requête SQL
val req = """
SELECT event_id, mention_id, country, language, event_day, event_month, event_year, COUNT(*) AS total
FROM event, mentions
WHERE event.event_id = mentions.event_id
GROUP BY event_id, mention_id, country, language, event_day, event_month, event_year
"""

// execution de la requête sur le DF
val table_ab = spark.sql(req)

// affichage de la table
//table_ab.show()

// creation de la nouvelle table
table_ab.createCassandraTable("reponses", "table_ab", partitionKeyColumns = Some(Seq("country")), clusteringKeyColumns = Some(Seq("event_day", "event_month", "event_year")))

// insertion des valeurs dans la nouvelle table
req1.write.cassandraFormat("table_ab", "reponses", "").mode("append").save()

// alternative avec "overwrite"
//req1.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(keyspace="test",table="question1").save()
