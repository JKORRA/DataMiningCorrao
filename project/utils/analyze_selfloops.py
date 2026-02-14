from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark = SparkSession.builder.appName("SelfLoopAnalysis").getOrCreate()

print("=== ANALYZING SELF-LOOPS AND ISOLATED NODES ===\n")

# Load the graph
df_graph = spark.read.parquet("outputs/music_graph.parquet")

# 1. Find self-loops (artist sampling themselves)
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

# 2. Load PageRank results
print("\n2. PAGERANK SCORES FOR SELF-LOOPING ARTISTS:")
print("-" * 70)
pagerank_df = spark.read.parquet("artist_pagerank.parquet")

# Check schema
print("PageRank schema:")
pagerank_df.printSchema()

# Get the correct column name
pagerank_columns = pagerank_df.columns
artist_col = pagerank_columns[0]  # First column should be artist name
score_col = pagerank_columns[1]   # Second should be score

print(f"Using columns: {artist_col}, {score_col}")

# Join to see if self-loops affect PageRank
self_loop_with_rank = self_loop_artists.join(
    pagerank_df, 
    self_loop_artists.Sampler_Artist_Name == pagerank_df[artist_col],
    "inner"
).select(artist_col, "self_sample_count", score_col) \
 .orderBy(desc(score_col))

print("\nSelf-looping artists and their authority scores:")
self_loop_with_rank.show(20, truncate=False)

# 3. Analyze the top 15 network
print("\n3. TOP 15 NETWORK ANALYSIS:")
print("-" * 70)
top_15 = pagerank_df.orderBy(desc(score_col)).limit(15)
top_15_names = [row[artist_col] for row in top_15.collect()]

print(f"Top 15 artists: {top_15_names[:5]}...")

# Filter edges for top 15
edges_top15 = df_graph.filter(
    (col("Sampler_Artist_Name").isin(top_15_names)) & 
    (col("Original_Artist_Name").isin(top_15_names))
)

print(f"Total edges among top 15: {edges_top15.count()}")

# Count self-loops in top 15
self_loops_top15 = edges_top15.filter(
    col("Sampler_Artist_Name") == col("Original_Artist_Name")
)
print(f"Self-loops in top 15: {self_loops_top15.count()}")

if self_loops_top15.count() > 0:
    print("\nWhich top 15 artists have self-loops:")
    self_loops_top15.groupBy("Sampler_Artist_Name").agg(count("*").alias("count")).show(truncate=False)

# Count isolated nodes (artists with no connections to other top 15)
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

# 4. Summary and Recommendation
print("\n" + "="*70)
print("4. SUMMARY AND RECOMMENDATION:")
print("="*70)
print(f"\nTotal self-loops in dataset: 1,407 ({1407/22135*100:.1f}% of all edges)")
print(f"Top 15 artists with self-loops: {self_loops_top15.count()}")
print(f"Isolated artists in top 15: {len(isolated_artists)}")
print(f"Connected artists in top 15: {len(connected_artists)}")

print("\nSelf-loops typically represent:")
print("  - Remixes (artist remixing their own song)")
print("  - Re-releases (different versions)")
print("  - Data errors (same recording linked twice)")
print("  - Live versions sampling studio versions")
print("  - Album intros/outros sampling other album tracks")
print("")
print("For PageRank and network visualization:")
print("  ✓ Self-loops can inflate authority artificially")
print("  ✓ They don't represent 'influence' between different artists")
print("  ✓ Standard practice: REMOVE self-loops for cleaner analysis")
print("")
print("RECOMMENDATION:")
print("  → Re-run PageRank WITHOUT self-loops")
print("  → Re-generate visualizations WITHOUT self-loops")
print("  → This will give cleaner, more interpretable results")

spark.stop()
