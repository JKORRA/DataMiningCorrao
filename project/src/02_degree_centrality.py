"""
Degree Centrality Analysis

This script analyzes the pre-processed music sampling graph to identify
influential artists using basic network degree centrality metrics.

The pipeline performs the following:
1. Calculates In-Degree Centrality (most sampled artists).
2. Calculates Out-Degree Centrality (artists who sample the most).
3. Identifies the specific individual songs that have been sampled the most.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count

spark = SparkSession.builder.appName("MusicGenealogy_Analysis").getOrCreate()

print("Loading saved graph...")
df = spark.read.parquet("outputs/music_graph.parquet")

df.cache()

# Most influential artists (In-Degree Centrality)
# High In-Degree indicates artists whose work serves as foundational material for others.
print("\n--- TOP 20 MOST SAMPLED ARTISTS ---")
print("These artists are the most influential through being sampled by others")

top_sampled = df.filter(col("Original_Artist_Name") != "[unknown]").groupBy("Original_Artist_Name") \
    .agg(count("*").alias("times_sampled")) \
    .orderBy(desc("times_sampled"))

top_sampled.show(20, truncate=False)

# Serial samplers (Out-Degree Centrality)
# High Out-Degree indicates prolific use of sampling, typical of electronic producers and DJs.
print("\n--- TOP 20 SERIAL SAMPLERS ---")
print("These artists use the most samples in their work (DJs and Producers)")

top_samplers = df.filter(col("Sampler_Artist_Name") != "[unknown]").groupBy("Sampler_Artist_Name") \
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
