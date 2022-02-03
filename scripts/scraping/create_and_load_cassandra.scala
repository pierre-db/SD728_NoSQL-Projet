import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

// chargement des fichiers dans des DF
val event = spark.read.format("csv").options(Map("header"->"true", "delimiter" -> "\t", "encoding" -> "ISO-8859-1", "inferSchema" -> "true")).load("*.export.CSV")
val mentions = spark.read.format("csv").options(Map("header"->"true", "delimiter" -> "\t", "encoding" -> "ISO-8859-1", "inferSchema" -> "true")).load("*.mentions.CSV")
val gkg = spark.read.format("csv").options(Map("header"->"true", "delimiter" -> "\t", "encoding" -> "ISO-8859-1", "inferSchema" -> "true")).load("*.gkg.csv")

// créationde vues pour interragir en SQL
event.createOrReplaceTempView("event")
mentions.createOrReplaceTempView("mentions")
gkg.createOrReplaceTempView("gkg")

// définition de la requête SQL table_ab
val req_table_ab = """
SELECT event_id, mention_id, country, language, event_day, event_month, event_year, COUNT(*) AS total
FROM event, mentions
WHERE event.event_id = mentions.event_id
GROUP BY event_id, mention_id, country, language, event_day, event_month, event_year
"""

// execution de la requête sur le DF
val table_ab = spark.sql(req_table_ab)

// affichage de la table
//table_ab.show()

// creation de la nouvelle table
table_ab.createCassandraTable("reponses", "table_ab", partitionKeyColumns = Some(Seq("country")), clusteringKeyColumns = Some(Seq("event_day", "event_month", "event_year")))

// insertion des valeurs dans la nouvelle table
table_ab.write.cassandraFormat("table_ab", "reponses", "").mode("append").save()

// définition de la requête SQL table_c
val req_table_c = """
SELECT source, theme, person, location, day, month, year, COUNT(*) as total, SUM(tone) as sum_tone
FROM gkg
GROUP BY source, theme, person, location, day, month, year
"""

// execution de la requête sur le DF
val table_c = spark.sql(req_table_c)

// creation de la nouvelle table
table_c.createCassandraTable("reponses", "table_c", partitionKeyColumns = Some(Seq("source")), clusteringKeyColumns = Some(Seq("theme", "person", "location", "day", "month", "year")))

// insertion des valeurs dans la nouvelle table
table_c.write.cassandraFormat("table_c", "reponses", "").mode("append").save()


// définition de la requête SQL table_d
val req_table_d = """
SELECT location, language, day, month, year, COUNT(*) as total, SUM(tone) as sum_tone
FROM gkg
GROUP BY location, language, day, month, year
"""

// execution de la requête sur le DF
val table_d = spark.sql(req_table_d)

// creation de la nouvelle table
table_d.createCassandraTable("reponses", "table_d", partitionKeyColumns = Some(Seq("location", "language")), clusteringKeyColumns = Some(Seq("day", "month", "year")))

// insertion des valeurs dans la nouvelle table
table_d.write.cassandraFormat("table_d", "reponses", "").mode("append").save()