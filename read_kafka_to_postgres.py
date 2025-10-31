import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, lit, when, abs, current_timestamp, trim, upper
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)

# --- 1. Versions et packages ---
KAFKA_VERSION = "3.5.0"
POSTGRES_VERSION = "42.7.3"

KAFKA_PACKAGES = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{KAFKA_VERSION}"
POSTGRES_PACKAGES = f"org.postgresql:postgresql:{POSTGRES_VERSION}"

# --- 2. Schémas ---
GEOMETRY_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("coordinates", ArrayType(DoubleType()), True)
])

AIRPORT_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("icaoCode", StringType(), True),
    StructField("iataCode", StringType(), True),
    StructField("geometry", GEOMETRY_SCHEMA, True)
])

ENVELOPE_SCHEMA = StructType([
    StructField("limit", IntegerType(), True),
    StructField("totalCount", IntegerType(), True),
    StructField("totalPages", IntegerType(), True),
    StructField("page", IntegerType(), True),
    StructField("items", ArrayType(AIRPORT_SCHEMA), True),
    StructField("source_data", StringType(), True)
])

# --- 3. SparkSession ---
spark = (
    SparkSession.builder
    .appName("AirportDataProcessing")
    .config("spark.jars.packages", f"{KAFKA_PACKAGES},{POSTGRES_PACKAGES}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session créée.")

# --- 4. Lecture Kafka ---
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "test-topic")
    .option("startingOffsets", "latest")
    .load()
)

# --- 5. Transformation des données ---
processed_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), ENVELOPE_SCHEMA).alias("data"))
    # Propager source_data avant explode
    .select(
        col("data.source_data").alias("source_data"),
        explode(col("data.items")).alias("airport")
    )
    .select(
        col("airport.name").alias("name"),
        col("airport.icaoCode").alias("icao"),
        col("airport.iataCode").alias("iata"),
        col("airport.country").alias("country"),
        col("airport.geometry.coordinates")[0].alias("longitude"),
        col("airport.geometry.coordinates")[1].alias("latitude"),
        col("source_data")
    )
)

# Remplacer les valeurs nulles par "N/A"
processed_df = processed_df.fillna("N/A", subset=["name", "icao", "iata", "country"])

# --- 6. Colonnes supplémentaires / calculées ---
processed_df = (
    processed_df
    .withColumn(
        "altitude_estimee_m",
        when(abs(col("latitude")) < 10, lit(50))
        .when(abs(col("latitude")) < 30, lit(150))
        .when(abs(col("latitude")) < 50, lit(300))
        .otherwise(lit(500))
    )
    .withColumn("timezone_approx", (col("longitude") / 15).cast("int"))
    .withColumn(
        "climat",
        when(abs(col("latitude")) < 10, lit("Tropical"))
        .when(abs(col("latitude")) < 30, lit("Aride"))
        .when(abs(col("latitude")) < 50, lit("Tempéré"))
        .otherwise(lit("Polaire"))
    )
    .withColumn(
        "airport_category",
        when(col("icao").startswith("K"), lit("North America"))
        .when(col("icao").startswith("E"), lit("Europe"))
        .when(col("icao").startswith("S"), lit("South America"))
        .when(col("icao").startswith("Z"), lit("Asia"))
        .when(col("icao").startswith("Y"), lit("Oceania"))
        .otherwise(lit("Unknown"))
    )
    .withColumn("name_clean", trim(col("name")))
    .withColumn("name_upper", upper(col("name")))
    .withColumn("timestamp_ingestion", current_timestamp())
    .withColumn(
        "processing_delay_s",
        (current_timestamp().cast("long") - col("timestamp_ingestion").cast("long"))
    )
    .withColumn(
        "geo_score",
        (abs(col("latitude")) / 90 + abs(col("longitude")) / 180)
    )
)

# --- 7. PostgreSQL ---
pg_url = "jdbc:postgresql://postgres:5432/mydb"
pg_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Liste des sources
sources = ["airports", "reportingpoints", "airspaces", "hotspots",
           "navaids", "hangglidings", "obstacles", "airfields"]

# Fonction pour écrire dans PostgreSQL (mode append)
def write_to_postgres(batch_df, batch_id, table_name):
    if batch_df.count() > 0:
        batch_df.write.jdbc(url=pg_url, table=table_name, mode="append", properties=pg_properties)
        print(f"✅ Batch {batch_id} écrit dans {table_name} ({batch_df.count()} lignes).")
    else:
        print(f"⚠️ Batch {batch_id} vide pour {table_name}, rien n’a été écrit.")

# --- 8. Streaming vers PostgreSQL ---
query = (
    processed_df.writeStream
    .foreachBatch(lambda df, batch_id: [
        write_to_postgres(df.filter(col("source_data") == src), batch_id, src) for src in sources
    ])
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

# --- 9. Streaming console (debug) ---
console_query = (
    processed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

print("🚀 Streaming démarré (console + PostgreSQL)...")
query.awaitTermination()
console_query.awaitTermination()
