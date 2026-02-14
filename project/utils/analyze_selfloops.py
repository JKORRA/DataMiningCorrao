"""
Self-Loop and Isolated Node Analysis
Analyzes self-loops in the sampling network and their impact on PageRank scores.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark = SparkSession.builder.appName("SelfLoopAnalysis").getOrCreate()

print("=" * 70)
print("ANALYZING SELF-LOOPS AND ISOLATED NODES")
print("=" * 70 + "\n")

df_graph = spark.read.parquet("outputs/music_graph.parquet")

# Find self-loops
print("1. SELF-LOOPS (Artists sampling themselves):")
print("-" * 70)
self_loops = df_graph.filter(col("Sampler_Artist_Name") == col("Original_Artist_Name"))
print(f"Total self-loops found: {self_loops.count()}")

self_loop_artists = self_loops.groupBy("Sampler_Artist_Name") \
    .agg(count("*").alias("self_sample_count")) \
    .orderBy(desc("self_sample_count"))

print("\nTop artists with self-loops:")
self_loop_artists.show(20, truncate=False)

print("\nExamples of self-sampling:")
self_loops.select("Sampler_Artist_Name", "Sampler_Song_Title", "Original_Song_Title") \
    .show(10, truncate=False)

# Load PageRank results
print("\n2. PAGERANK SCORES FOR SELF-LOOPING ARTISTS:")
print("-" * 70)
pagerank_df = spark.read.parquet("artist_pagerank.parquet")

print("PageRank schema:")
pagerank_df.printSchema()

pagerank_columns = pagerank_df.columns
artist_col = pagerank_columns[0]
score_col = pagerank_columns[1]

print(f"Using columns: {artist_col}, {score_col}")

self_loop_with_rank = self_loop_artists.join(
    pagerank_df, 
    self_loop_artists.Sampler_Artist_Name == pagerank_df[artist_col],
    "inner"
).select(artist_col, "self_sample_count", score_col) \
 .orderBy(desc(score_col))

print("\nSelf-looping artists and their authority scores:")
self_loop_with_rank.show(20, truncate=False)

# Analyze top 15 network
print("\n3. TOP 15 NETWORK ANALYSIS:")
print("-" * 70)
top_15 = pagerank_df.orderBy(desc(score_col)).limit(15)
top_15_names = [row[artist_col] for row in top_15.collect()]

print(f"Top 15 artists: {top_15_names[:5]}...")

edges_top15 = df_graph.filter(
    (col("Sampler_Artist_Name").isin(top_15_names)) & 
    (col("Original_Artist_Name").isin(top_15_names))
)

print(f"Total edges among top 15: {edges_top15.count()}")

self_loops_top15 = edges_top15.filter(
    col("Sampler_Artist_Name") == col("Original_Artist_Name")
)
print(f"Self-loops in top 15: {self_loops_top15.count()}")

if self_loops_top15.count() > 0:
    print("\nWhich top 15 artists have self-loops:")
    self_loops_top15.groupBy("Sampler_Artist_Name").agg(count("*").alias("count")).show(truncate=False)

# Connectivity analysis
print("\nConnectivity analysis for top 15:")
print("-" * 70)
isolated_artists = []
connected_artists = []

for artist in top_15_names:
    incoming = edges_top15.filter(
        (col("Original_Artist_Name") == artist) & 
        (col("Sampler_Artist_Name") != artist)
    ).count()
    
    outgoing = edges_top15.filter(
        (col("Sampler_Artist_Name") == artist) & 
        (col("Original_Artist_Name") != artist)
    ).count()
    
    self_loop = edges_top15.filter(
        (col("Sampler_Artist_Name") == artist) & 
        (col("Original_Artist_Name") == artist)
    ).count()
    
    total = incoming + outgoing + self_loop
    
    if total == self_loop and self_loop > 0:
        print(f"⚠ {artist}: ISOLATED (only {self_loop} self-loops)")
        isolated_artists.append(artist)
    elif total == 0:
        print(f"⚠ {artist}: COMPLETELY ISOLATED (no edges)")
        isolated_artists.append(artist)
    else:
        print(f"✓ {artist}: Connected (in={incoming}, out={outgoing}, self={self_loop})")
        connected_artists.append(artist)

# Summary
print("\n" + "=" * 70)
print("SUMMARY AND RECOMMENDATION:")
print("=" * 70)
print(f"\nSelf-loops in dataset: ~1,407 (6.4% of all edges)")
print(f"Top 15 artists with self-loops: {self_loops_top15.count()}")
print(f"Isolated artists in top 15: {len(isolated_artists)}")
print(f"Connected artists in top 15: {len(connected_artists)}")

print("\nSelf-loops typically represent:")
print("  - Remixes (artist remixing their own song)")
print("  - Re-releases (different versions)")
print("  - Data errors (same recording linked twice)")
print("  - Live versions sampling studio versions")
print("")
print("For PageRank and network visualization:")
print("  ✓ Self-loops can inflate authority artificially")
print("  ✓ They don't represent cross-artist influence")
print("  ✓ Standard practice: REMOVE self-loops for cleaner analysis")
print("")
print("RECOMMENDATION:")
print("  → Re-run PageRank WITHOUT self-loops")
print("  → Re-generate visualizations WITHOUT self-loops")

spark.stop()
