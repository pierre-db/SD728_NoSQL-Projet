from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col 
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.types import StringType


spark = SparkSession.builder.master("local[*]").appName("SparkWorker").getOrCreate()

# schema = StructType([StructField('GlobalEventID', StringType(), False),
#                     StructField('Day', IntegerType(), True),
#                     StructField('MonthYear', IntegerType(), True),
#                     StructField('ActionGeoCountryCode', StringType(), True),
#                     StructField('GoldsteinScale', FloatType(), True)
#                    ])

event = spark.read.format("csv").options(header=False, delimiter="\t", encoding="ISO-8859-1").load("../../data/20220202123000.export.CSV") \
                  .select(col("_c0"), col("_c1"), col("_c2"), col("_c30"), col("_c53")) \
                  .withColumn("_c0", col("_c0").cast("long")).withColumn("_c1", col("_c1").cast("long")).withColumn("_c2", col("_c2").cast("long")).withColumn("_c30", col("_c30").cast("float")) \
                  .withColumnRenamed("_c0","GlobalEventID").withColumnRenamed("_c1","Day").withColumnRenamed("_c2","MonthYear").withColumnRenamed("_c30","GoldsteinScale").withColumnRenamed("_c53","ActionGeoCountryCode")
#event = spark.createDataFrame(data = event.rdd, schema = schema)

udf_format_lang = udf(lambda x: x if x=="eng" else x[6:9],StringType())

mentions = spark.read.format("csv").options(header=False, delimiter="\t", encoding="ISO-8859-1").load("../../data/20220202123000.mentions.CSV") \
                  .select(col("_c0"), col("_c14")) \
                  .withColumn("_c0", col("_c0").cast("integer")) \
                  .withColumnRenamed("_c0","GlobalEventID").withColumnRenamed("_c14","MentionDocTranslationInfo") \
                  .na.fill('eng', subset=["MentionDocTranslationInfo"]) \
                  .withColumn("MentionDocTranslationInfo", udf_format_lang(col("MentionDocTranslationInfo")))

kb = spark.read.format("csv").options(header=False, delimiter="\t", encoding="ISO-8859-1").load("../../data/20220202123000.gkg.CSV") \
                  .select(col("_c1"), col("_c3"), col("_c7"), col("_c9"), col("_c15"), col("_c25")) \
                  .withColumn("_c1", col("_c1").cast("long")) \
                  .withColumnRenamed("_c1","V21DATE").withColumnRenamed("_c3","V2SOURCECOMMONNAME").withColumnRenamed("_c7","V1THEMES").withColumnRenamed("_c9","V1LOCATIONS").withColumnRenamed("_c15","V15TONE").withColumnRenamed("_c25","V21TRANSLATIONINFO") \
                  .na.fill('eng', subset=["V21TRANSLATIONINFO"]) \
                  .withColumn("V21TRANSLATIONINFO", udf_format_lang(col("V21TRANSLATIONINFO")))

kb.show()
kb.printSchema()
