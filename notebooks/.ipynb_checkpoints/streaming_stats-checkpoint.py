"""
Spark Structured Streaming : statistiques par zone toutes les 30 s.
Lit Kafka (taxi-locations + client-requests), calcule :
  - taxis disponibles / en course par zone
  - demandes en attente par zone
  - ratio demande / offre (indicateur de "surge pricing")
et renvoie le resultat dans Kafka (topic zone-stats) -> Logstash -> Kibana.

Dans Jupyter :  %run streaming_stats.py   (ou copier dans une cellule)
"""
import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, DoubleType

# Package Kafka adapte a la version de Spark de l'image
spark_ver = pyspark.__version__
scala_ver = "2.13" if spark_ver.startswith("4") else "2.12"
kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_ver}:{spark_ver}"

spark = (SparkSession.builder
         .appName("uber-zone-stats")
         .config("spark.jars.packages", kafka_pkg)
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

BOOTSTRAP = "kafka:29092"

taxi_schema = (StructType()
               .add("taxi_id", StringType()).add("status", StringType())
               .add("zone", StringType()).add("lat", DoubleType())
               .add("lon", DoubleType()).add("timestamp", StringType()))

client_schema = (StructType()
                 .add("request_id", StringType()).add("status", StringType())
                 .add("zone", StringType()).add("trip_km", DoubleType())
                 .add("timestamp", StringType()))


def read_topic(topic, schema):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
            .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
            .select("d.*")
            .withColumn("ts", F.to_timestamp("timestamp")))


taxis = read_topic("taxi-locations", taxi_schema)
clients = read_topic("client-requests", client_schema)

WINDOW = "30 seconds"

taxi_stats = (taxis.withWatermark("ts", "1 minute")
              .groupBy(F.window("ts", WINDOW), "zone")
              .agg(F.approx_count_distinct(F.when(F.col("status") == "available", F.col("taxi_id"))).alias("taxis_available"),
                   F.approx_count_distinct(F.when(F.col("status") != "available", F.col("taxi_id"))).alias("taxis_busy"))
              .withColumn("type", F.lit("taxis")))

client_stats = (clients.withWatermark("ts", "1 minute")
                .groupBy(F.window("ts", WINDOW), "zone")
                .agg(F.approx_count_distinct(F.when(F.col("status") == "waiting", F.col("request_id"))).alias("clients_waiting"),
                     F.approx_count_distinct(F.when(F.col("status") == "completed", F.col("request_id"))).alias("trips_completed"),
                     F.approx_count_distinct(F.when(F.col("status") == "cancelled", F.col("request_id"))).alias("trips_cancelled"),
                     F.round(F.avg("trip_km"), 2).alias("avg_trip_km"))
                .withColumn("type", F.lit("clients")))


def to_kafka(df, name):
    out = (df.withColumn("timestamp", F.date_format(F.col("window.end"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
             .drop("window")
             .select(F.col("zone").alias("key"), F.to_json(F.struct("*")).alias("value")))
    return (out.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("topic", "zone-stats")
            .option("checkpointLocation", f"/tmp/checkpoints/{name}")
            .outputMode("update")
            .start())


q1 = to_kafka(taxi_stats, "taxi_stats")
q2 = to_kafka(client_stats, "client_stats")

# Affichage console pour verifier
q3 = (client_stats.writeStream.format("console")
      .option("truncate", "false").outputMode("update").start())

print("Streaming lance. Spark UI : http://localhost:4040")
# Pour arreter : for q in spark.streams.active: q.stop()
