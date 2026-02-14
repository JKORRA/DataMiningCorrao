import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("MusicGenealogy_GraphBuilder") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

BASE_PATH = "mbdump" 

# --- SCHEMI ---
recording_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("gid", StringType(), True),
    StructField("name", StringType(), True),
    StructField("artist_credit", IntegerType(), True),
    StructField("length", IntegerType(), True),
    StructField("comment", StringType(), True),
    StructField("edits_pending", IntegerType(), True),
    StructField("last_updated", StringType(), True),
    StructField("video", StringType(), True)
])

artist_credit_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("gid", StringType(), True),
    StructField("edits_pending", IntegerType(), True),
    StructField("last_updated", StringType(), True),
    StructField("is_deleted", StringType(), True)
])

link_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("link_type", IntegerType(), True),
    StructField("begin_date_year", StringType(), True),
    StructField("begin_date_month", StringType(), True),
    StructField("begin_date_day", StringType(), True),
    StructField("end_date_year", StringType(), True),
    StructField("end_date_month", StringType(), True),
    StructField("end_date_day", StringType(), True),
    StructField("attribute_count", IntegerType(), True),
    StructField("created", StringType(), True),
    StructField("ended", StringType(), True)
])

l_rec_rec_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("link", IntegerType(), True),
    StructField("entity0", IntegerType(), True),
    StructField("entity1", IntegerType(), True),
    StructField("edits_pending", IntegerType(), True),
    StructField("last_updated", StringType(), True),
    StructField("link_order", IntegerType(), True)
])

def load_mb_table(filename, schema):
    return spark.read.option("delimiter", "\t").option("nullValue", "\\N").schema(schema).csv(f"{BASE_PATH}/{filename}")

# --- CARICAMENTO ---
print("Caricamento tabelle...")
df_recording = load_mb_table("recording", recording_schema).select("id", "name", "artist_credit")
df_artist_credit = load_mb_table("artist_credit", artist_credit_schema).select("id", "name")
df_link = load_mb_table("link", link_schema).select("id", "link_type")
df_l_rec_rec = load_mb_table("l_recording_recording", l_rec_rec_schema).select("link", "entity0", "entity1")

# --- COSTRUZIONE ---
print("Filtraggio relazioni...")
TARGET_LINK_TYPES = [69, 231]

sampling_edges = df_l_rec_rec.join(df_link, df_l_rec_rec.link == df_link.id) \
    .filter(col("link_type").isin(TARGET_LINK_TYPES)) \
    .select(col("entity0").alias("source_song_id"), col("entity1").alias("target_song_id"))

print("Arricchimento (Join)...")
df_source_info = sampling_edges.join(df_recording.alias("src_rec"), sampling_edges.source_song_id == col("src_rec.id")) \
                               .join(df_artist_credit.alias("src_art"), col("src_rec.artist_credit") == col("src_art.id")) \
                               .select(
                                   col("source_song_id"),
                                   col("target_song_id"),
                                   col("src_rec.name").alias("Sampler_Song_Title"),
                                   col("src_art.name").alias("Sampler_Artist_Name")
                               )

df_final_graph = df_source_info.join(df_recording.alias("tgt_rec"), df_source_info.target_song_id == col("tgt_rec.id")) \
                               .join(df_artist_credit.alias("tgt_art"), col("tgt_rec.artist_credit") == col("tgt_art.id")) \
                               .select(
                                   # QUÍ STA LA CORREZIONE: MANTENIAMO GLI ID
                                   col("source_song_id"),
                                   col("target_song_id"),
                                   col("Sampler_Artist_Name"),
                                   col("Sampler_Song_Title"),
                                   col("tgt_art.name").alias("Original_Artist_Name"),
                                   col("tgt_rec.name").alias("Original_Song_Title")
                               )

# CRITICAL IMPROVEMENT: Remove self-loops (artist sampling themselves)
# Self-loops represent remixes, re-releases, or data errors - not cross-artist influence
edges_before = df_final_graph.count()
print(f"Total edges before self-loop removal: {edges_before}")
df_final_graph = df_final_graph.filter(col("Sampler_Artist_Name") != col("Original_Artist_Name"))
edges_after = df_final_graph.count()
print(f"Edges after removing self-loops: {edges_after}")
print(f"Self-loops removed: {edges_before - edges_after} edges")

print(f"Salvataggio {edges_after} righe...")
df_final_graph.write.mode("overwrite").parquet("outputs/music_graph.parquet")
print("Fatto. Ora puoi lanciare pagerank_manual.py")
spark.stop()
