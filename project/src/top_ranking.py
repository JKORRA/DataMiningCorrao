"""
Music Genealogy Analysis
Analyzes the music sampling graph to identify influential artists and patterns.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count

spark = SparkSession.builder.appName("MusicGenealogy_Analysis").getOrCreate()

print("Loading saved graph...")
df = spark.read.parquet("outputs/music_graph.parquet")

df.cache()

# Most influential artists (In-Degree Centrality)
print("\n--- TOP 20 MOST SAMPLED ARTISTS ---")
print("These artists are the most influential through being sampled by others")

top_sampled = df.groupBy("Original_Artist_Name") \
    .agg(count("*").alias("times_sampled")) \
    .orderBy(desc("times_sampled"))

top_sampled.show(20, truncate=False)

# Serial samplers (Out-Degree Centrality)
print("\n--- TOP 20 SERIAL SAMPLERS ---")
print("These artists use the most samples in their work (DJs and Producers)")

top_samplers = df.groupBy("Sampler_Artist_Name") \
    .agg(count("*").alias("samples_used")) \
    .orderBy(desc("samples_used"))

top_samplers.show(20, truncate=False)

# Most sampled songs
print("\n--- TOP 10 MOST SAMPLED SONGS ---")
print("Individual songs that have been sampled the most")

top_songs = df.groupBy("Original_Artist_Name", "Original_Song_Title") \
    .agg(count("*").alias("times_used")) \
    .orderBy(desc("times_used"))

top_songs.show(10, truncate=False)

spark.stop()
