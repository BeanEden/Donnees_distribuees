from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, from_json, from_unixtime, to_date, round, year, month, date_format
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType
)

# =============================
# 1️⃣ CONFIGURATION
# =============================
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"
PG_URL = "jdbc:postgresql://postgres:5432/postgres"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "opensky_states"

OPENSKY_FLAT_SCHEMA = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", LongType(), True),
    StructField("last_contact", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", LongType(), True),
    StructField("category", DoubleType(), True),
])


# Propriétés JDBC (utilisées uniquement si tu veux écrire via Spark JDBC)
PG_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
    "reWriteBatchedInserts": "true",
    "loggerLevel": "DEBUG"
}



# =============================
# 2️⃣ FONCTION D'ÉCRITURE
# =============================
def write_to_pg(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"\n⚠️ Batch {batch_id} vide, rien à insérer.")
        return

    print(f"\n=== 🚀 DÉBUT DU BATCH {batch_id} ({batch_df.count()} lignes) ===")

    import psycopg2
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host="postgres",
            port=5432
        )
        cur = conn.cursor()

        # --- DIM_PAYS ---
        df_pays = batch_df.select(
            col("origin_country").alias("pk_pays"),
            col("origin_country").alias("nom_pays")
        ).distinct().dropna(subset=["pk_pays"])
        
        for row in df_pays.collect():
            cur.execute("""
                INSERT INTO dim_pays(pk_pays, nom_pays)
                VALUES (%s, %s)
                ON CONFLICT (pk_pays) DO UPDATE
                SET nom_pays = EXCLUDED.nom_pays
            """, (row.pk_pays, row.nom_pays))
        conn.commit()
        print(f"✅ DIM_PAYS : {df_pays.count()} lignes upsertées")

        # --- DIM_VOL ---
        df_vol = batch_df.select(
            col("callsign").alias("pk_vol"),
            col("callsign"),
            col("on_ground"),
            col("squawk")
        ).distinct().filter(col("pk_vol").isNotNull())

        for row in df_vol.collect():
            cur.execute("""
                INSERT INTO dim_vol(pk_vol, callsign, on_ground, squawk)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pk_vol) DO UPDATE
                SET callsign = EXCLUDED.callsign,
                    on_ground = EXCLUDED.on_ground,
                    squawk = EXCLUDED.squawk
            """, (row.pk_vol, row.callsign, row.on_ground, row.squawk))
        conn.commit()
        print(f"✅ DIM_VOL : {df_vol.count()} lignes upsertées")

       # --- DIM_TEMPS ---
        df_temps = batch_df.select(
            col("time_position").alias("pk_temps"),
            to_date(col("time_position")).alias("date_capture"),
            col("time_position").alias("heure_capture"),
            year(col("time_position")).alias("annee_capture"),
            month(col("time_position")).alias("mois_capture"),
            date_format(col("time_position"), "EEEE").alias("jour_capture")
        ).distinct().dropna(subset=["pk_temps"])

        for row in df_temps.collect():
            cur.execute("""
                INSERT INTO dim_temps(
                    pk_temps, date_capture, heure_capture, annee_capture, mois_capture, jour_capture
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (pk_temps) DO UPDATE
                SET date_capture = EXCLUDED.date_capture,
                    heure_capture = EXCLUDED.heure_capture,
                    annee_capture = EXCLUDED.annee_capture,
                    mois_capture = EXCLUDED.mois_capture,
                    jour_capture = EXCLUDED.jour_capture
            """, (
                row.pk_temps, 
                row.date_capture, 
                row.heure_capture, 
                row.annee_capture, 
                row.mois_capture, 
                row.jour_capture
            ))
        conn.commit()
        print(f"✅ DIM_TEMPS : {df_temps.count()} lignes upsertées")


        # --- DIM_AERONEF ---
        df_aeronef = batch_df.select(
            col("icao24").alias("pk_aeronef"),
            col("category")  # <-- nouveau champ
        ).distinct().dropna(subset=["pk_aeronef"])

        for row in df_aeronef.collect():
            cur.execute("""
                INSERT INTO dim_aeronef(pk_aeronef, category)
                VALUES (%s, %s)
                ON CONFLICT (pk_aeronef) DO UPDATE
                SET category = EXCLUDED.category
            """, (row.pk_aeronef, row.category))
        conn.commit()
        print(f"✅ DIM_AERONEF : {df_aeronef.count()} lignes upsertées")

        # --- DIM_POSITION ---
        df_pos = batch_df.select(
            concat(
                col("latitude").cast("string"), lit("_"),
                col("longitude").cast("string")
            ).alias("pk_position"),
            col("latitude"),
            col("longitude"),
            col("geo_altitude"),
            col("on_ground"),
            col("position_source")
        ).distinct().dropna(subset=["pk_position"])

        for row in df_pos.collect():
            cur.execute("""
                INSERT INTO dim_position(pk_position, latitude, longitude, geo_altitude, on_ground, position_source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (pk_position) DO UPDATE
                SET latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    geo_altitude = EXCLUDED.geo_altitude,
                    on_ground = EXCLUDED.on_ground,
                    position_source = EXCLUDED.position_source
            """, (row.pk_position, row.latitude, row.longitude, row.geo_altitude, row.on_ground, row.position_source))
        conn.commit()
        print(f"✅ DIM_POSITION : {df_pos.count()} lignes upsertées")

        # --- FAIT_ETAT_VOL ---
        df_fait = batch_df.select(
            concat(
                col("callsign"),lit("_"),col("time_position").cast("string"), lit("_"),
                col("latitude").cast("string"), lit("_"),
                col("longitude").cast("string")).alias("pk_etat"),            
            col("time_position").alias("fk_temps"),
            col("icao24").alias("fk_aeronef"),
            concat(
                col("latitude").cast("string"), lit("_"),
                col("longitude").cast("string")
            ).alias("fk_position"),
            col('origin_country').alias("fk_pays"),
            col("longitude"),
            col("latitude"),
            col("baro_altitude"),
            col("geo_altitude"),
            col("velocity"),
            col("vertical_rate"),
            col("true_track"),
            col("on_ground"),
            col("spi"),
            col("position_source"),
            col("last_contact")
        ).dropna(subset=["fk_temps", "fk_aeronef", "fk_position", "fk_pays"])
        

        for row in df_fait.collect():
            cur.execute("""
                INSERT INTO fait_etat_vol(
                    fk_temps, fk_aeronef, fk_position,fk_pays,
                    baro_altitude, geo_altitude,
                    velocity, vertical_rate, true_track,
                    on_ground, spi, position_source, last_contact
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                row.fk_temps, row.fk_aeronef, row.fk_position, row.fk_pays,
                row.baro_altitude, row.geo_altitude,
                row.velocity, row.vertical_rate, row.true_track,
                row.on_ground, row.spi, row.position_source, row.last_contact
            ))
        conn.commit()
        print(f"✅ FAIT_ETAT_VOL : {df_fait.count()} lignes insérées")

        cur.close()
        conn.close()
        print(f"🎯 Batch {batch_id} écrit avec succès dans toutes les tables !")

    except Exception as e:
        print(f"\n🚨 ERREUR FATALE PENDANT L'UPSERT DU BATCH {batch_id} 🚨")
        print(f"Détails de l'erreur: {e}")
        raise e

# =============================
# 3️⃣ STREAMING SPARK + KAFKA
# =============================
spark = SparkSession.builder.appName("OpenSkyFloconProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("🚀 Initialisation du pipeline Spark-Kafka-PostgreSQL...")

df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() 

df_json = df_raw.selectExpr("CAST(value AS STRING) AS value_str") \
    .select(from_json(col("value_str"), OPENSKY_FLAT_SCHEMA).alias("data")) \
    .select("data.*")

# Mapping des champs
df_mapped = df_json.select(
    from_unixtime(col("time_position")).cast("timestamp").alias("time_position"),
    col("icao24"),
    col("callsign"),
    col("origin_country"),
    col("longitude"),
    col("latitude"),
    col("baro_altitude"),
    col("geo_altitude"),
    col("velocity"),
    col("vertical_rate"),
    col("true_track"),
    col("on_ground"),
    col("spi"),
    col("position_source").cast("string").alias("position_source"),
    col("squawk"),
    lit(None).alias("category"),
    from_unixtime(col("last_contact")).cast("timestamp").alias("last_contact")
)

query = df_mapped.writeStream \
    .foreachBatch(write_to_pg) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark/checkpoints/opensky_flocon") \
    .start()
    
print("✅ Stream démarré avec succès, en attente des messages Kafka...")
query.awaitTermination()
