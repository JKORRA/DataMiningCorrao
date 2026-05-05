from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, broadcast, when, regexp_replace
spark = SparkSession.builder \
    .appName("MusicGenealogy_GraphBuilder") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

BASE_PATH = "mbdump"

# Define schemas for MusicBrainz tables
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
    StructField("artist_count", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("edits_pending", IntegerType(), True)
])

artist_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("gid", StringType(), True),
    StructField("name", StringType(), True),
    StructField("sort_name", StringType(), True)
])

artist_credit_name_schema = StructType([
    StructField("artist_credit", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("artist", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("join_phrase", StringType(), True)
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

track_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("gid", StringType(), True),
    StructField("recording", IntegerType(), True),
    StructField("medium", IntegerType(), True)
])

medium_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("release", IntegerType(), True)
])

release_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("gid", StringType(), True),
    StructField("name", StringType(), True),
    StructField("artist_credit", IntegerType(), True),
    StructField("release_group", IntegerType(), True)
])

release_group_meta_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("release_count", IntegerType(), True),
    StructField("first_release_date_year", IntegerType(), True)
])

def load_mb_table(filename, schema):
    return spark.read.option("delimiter", "\t").option("nullValue", "\\N").schema(schema).csv(f"{BASE_PATH}/{filename}")

# Load MusicBrainz tables
print("Loading MusicBrainz tables...")
df_recording = load_mb_table("recording", recording_schema).select("id", "name", "artist_credit")
df_artist = load_mb_table("artist", artist_schema).select(col("id").alias("artist_id"), "name", "gid")
df_acn = load_mb_table("artist_credit_name", artist_credit_name_schema).select("artist_credit", "position", "artist")

# Get primary canonical artist for each artist_credit
df_artist_credit = df_acn.filter(col("position") == 0) \
    .join(df_artist, df_acn.artist == df_artist.artist_id) \
    .select(col("artist_credit").alias("id"), col("name"), col("gid")) \
    .withColumn("name", regexp_replace(col("name"), "(?i)\\s+(&|feat\\.?|ft\\.?|with|and)\\s+.*", ""))

df_link = load_mb_table("link", link_schema).select("id", "link_type")
df_l_rec_rec = load_mb_table("l_recording_recording", l_rec_rec_schema).select("link", "entity0", "entity1")

df_track = load_mb_table("track", track_schema).select("recording", "medium")
df_medium = load_mb_table("medium", medium_schema).select("id", "release")
df_release = load_mb_table("release", release_schema).select("id", "release_group")
df_release_group_meta = load_mb_table("release_group_meta", release_group_meta_schema).select("id", "first_release_date_year")

# Compute the first release year for each recording
print("Computing release years...")
df_rec_year = df_track.join(df_medium, df_track.medium == df_medium.id) \
    .join(df_release, df_medium.release == df_release.id) \
    .join(df_release_group_meta, df_release.release_group == df_release_group_meta.id) \
    .groupBy("recording") \
    .agg({"first_release_date_year": "min"}) \
    .withColumnRenamed("min(first_release_date_year)", "release_year")

# Filter for sampling relationships (link types 69 and 231)
print("Filtering sampling relationships...")
TARGET_LINK_TYPES = [69, 231, 230, 232, 227]

sampling_edges = df_l_rec_rec.join(df_link, df_l_rec_rec.link == df_link.id) \
    .filter(col("link_type").isin(TARGET_LINK_TYPES)) \
    .withColumn("weight", when(col("link_type").isin([69, 231]), 1.0).otherwise(0.5)) \
    .select(col("entity0").alias("source_song_id"), col("entity1").alias("target_song_id"), col("weight"))

# Enrich edges with song and artist information
print("Enriching graph with artist and song names...")
df_source_info = sampling_edges.join(df_recording.alias("src_rec"), sampling_edges.source_song_id == col("src_rec.id")) \
                               .join(broadcast(df_artist_credit).alias("src_art"), col("src_rec.artist_credit") == col("src_art.id")) \
                               .join(df_rec_year.alias("src_yr"), sampling_edges.source_song_id == col("src_yr.recording"), "left") \
                               .select(
                                   col("source_song_id"),
                                   col("target_song_id"),
                                   col("weight"),
                                   col("src_rec.name").alias("Sampler_Song_Title"),
                                   col("src_art.name").alias("Sampler_Artist_Name"),
                                   col("src_art.gid").alias("Sampler_Artist_GID"),
                                   col("src_yr.release_year").alias("Sampler_Release_Year")
                               )

df_final_graph = df_source_info.join(df_recording.alias("tgt_rec"), df_source_info.target_song_id == col("tgt_rec.id")) \
                               .join(broadcast(df_artist_credit).alias("tgt_art"), col("tgt_rec.artist_credit") == col("tgt_art.id")) \
                               .join(df_rec_year.alias("tgt_yr"), df_source_info.target_song_id == col("tgt_yr.recording"), "left") \
                               .select(
                                   col("source_song_id"),
                                   col("target_song_id"),
                                   col("weight"),
                                   col("Sampler_Artist_Name"),
                                   col("Sampler_Artist_GID"),
                                   col("Sampler_Song_Title"),
                                   col("Sampler_Release_Year"),
                                   col("tgt_art.name").alias("Original_Artist_Name"),
                                   col("tgt_art.gid").alias("Original_Artist_GID"),
                                   col("tgt_rec.name").alias("Original_Song_Title"),
                                   col("tgt_yr.release_year").alias("Original_Release_Year")
                               )

# Remove self-loops (artists sampling themselves) and unknown artists
edges_before = df_final_graph.count()
print(f"Total edges before self-loop and unknown removal: {edges_before}")
df_final_graph = df_final_graph.filter((col("Sampler_Artist_GID") != col("Original_Artist_GID")) & (col("Sampler_Artist_Name") != "[unknown]") & (col("Original_Artist_Name") != "[unknown]"))
edges_after = df_final_graph.count()
print(f"Edges after removing self-loops: {edges_after}")
print(f"Self-loops removed: {edges_before - edges_after} edges")

# Save the final graph
print(f"Saving graph with {edges_after} edges...")
df_final_graph.write.mode("overwrite").parquet("outputs/music_graph.parquet")
print("Graph saved successfully. Run compute_authority_manual.py next.")
spark.stop()
