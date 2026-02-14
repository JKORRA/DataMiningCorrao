from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count

# 1. Carichiamo il Grafo Pulito (Molto più veloce dei CSV)
spark = SparkSession.builder.appName("MusicGenealogy_Analysis").getOrCreate()

print("Caricamento del grafo salvato...")
df = spark.read.parquet("outputs/music_graph.parquet")

# Cache in memoria perché faremo varie query su questo dataset
df.cache()

# =========================================================
# ANALISI 1: THE MOST INFLUENTIAL (In-Degree Centrality)
# =========================================================
# Chi sono gli artisti più campionati della storia?
print("\n--- TOP 20 ARTISTI PIÙ CAMPIONATI (I Padri Fondatori) ---")

top_sampled = df.groupBy("Original_Artist_Name") \
    .agg(count("*").alias("times_sampled")) \
    .orderBy(desc("times_sampled"))

top_sampled.show(20, truncate=False)

# =========================================================
# ANALISI 2: THE SERIAL SAMPLERS (Out-Degree Centrality)
# =========================================================
# Chi sono gli artisti che campionano di più?
print("\n--- TOP 20 SERIAL SAMPLERS (I Dj e Producer) ---")

top_samplers = df.groupBy("Sampler_Artist_Name") \
    .agg(count("*").alias("samples_used")) \
    .orderBy(desc("samples_used"))

top_samplers.show(20, truncate=False)

# =========================================================
# ANALISI 3: LE CANZONI PIÙ SACCHEGGIATE
# =========================================================
# Qual è la singola canzone più usata nella storia?
# (Spoiler: Probabilmente "Amen, Brother" o qualcosa di James Brown)
print("\n--- TOP 10 CANZONI PIÙ CAMPIONATE ---")

top_songs = df.groupBy("Original_Artist_Name", "Original_Song_Title") \
    .agg(count("*").alias("times_used")) \
    .orderBy(desc("times_used"))

top_songs.show(10, truncate=False)

spark.stop()
