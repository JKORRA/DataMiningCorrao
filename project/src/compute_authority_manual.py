from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum, count, when, abs as _abs, max as _max_fn

spark = SparkSession.builder \
    .appName("MusicGenealogy_PageRank") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoints")

print("Loading graph...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")

# Verify self-loops were removed during data preparation
self_loops = df_graph.filter(col("Sampler_Artist_Name") == col("Original_Artist_Name")).count()
if self_loops > 0:
    print(f"WARNING: Found {self_loops} self-loops! Removing them now...")
    df_graph = df_graph.filter(col("Sampler_Artist_Name") != col("Original_Artist_Name"))
else:
    print("✓ No self-loops detected")

print(f"Graph loaded: {df_graph.count()} edges")

# Create directed edges: Original Artist -> Sampler
# Authority flows from sampled artists to those who sample them
links = df_graph.select(
    col("target_song_id").alias("src"),
    col("source_song_id").alias("dst")
).distinct()

# Calculate out-degree (constant across iterations)
out_degrees = links.groupBy("src").agg(count("dst").alias("out_degree"))

# Initialize all nodes with rank 1.0
nodes = links.select("src").union(links.select("dst")).distinct().select(col("src").alias("id"))
ranks = nodes.withColumn("rank", lit(1.0))

# PageRank iteration parameters
MAX_ITERATIONS = 20
DAMPING = 0.85
TOLERANCE = 0.0001

print(f"Starting PageRank computation (convergence threshold: {TOLERANCE})...")

for i in range(MAX_ITERATIONS):
    print(f"--- Iteration {i+1}/{MAX_ITERATIONS} ---")
    
    old_ranks = ranks
    
    # Calculate contributions from each source node
    contribs = links.join(ranks, links.src == ranks.id) \
                    .join(out_degrees, links.src == out_degrees.src) \
                    .select(
                        col("dst"), 
                        (col("rank") / col("out_degree")).alias("contribution")
                    )
    
    # Sum contributions received by each destination node
    sum_contribs = contribs.groupBy("dst").agg(_sum("contribution").alias("sum_contrib"))
    
    # Apply PageRank formula: (1-d) + d * sum_contributions
    ranks_calculated = sum_contribs.select(
        col("dst").alias("id"),
        (lit(1 - DAMPING) + (lit(DAMPING) * col("sum_contrib"))).alias("rank")
    )
    
    # Handle dangling nodes (nodes with no incoming edges)
    ranks = ranks_calculated.join(nodes, "id", "right_outer") \
        .select(
            col("id"),
            when(col("rank").isNull(), lit(1 - DAMPING)).otherwise(col("rank")).alias("rank")
        )
    
    ranks = ranks.checkpoint()
    
    # Check convergence
    diff_check = ranks.join(old_ranks.withColumnRenamed("rank", "old_rank"), "id") \
        .select(_abs(col("rank") - col("old_rank")).alias("abs_diff"))
    
    max_diff = diff_check.agg(_max_fn("abs_diff").alias("max_diff")).collect()[0]["max_diff"]
    
    print(f"    Max rank change: {max_diff:.6f}")
    
    if max_diff < TOLERANCE:
        print(f"✓ Convergence reached after {i+1} iterations!")
        break

# Aggregate results by artist
print("Aggregating scores by artist...")

song_artist_map = df_graph.select(
    col("target_song_id").alias("id"), 
    col("Original_Artist_Name").alias("artist")
).distinct()

final_scores = ranks.join(song_artist_map, "id")

artist_authority = final_scores.groupBy("artist") \
    .agg(_sum("rank").alias("authority_score")) \
    .orderBy(col("authority_score").desc())

print("\n--- TOP 20 ARTISTS BY AUTHORITY ---")
artist_authority.show(20, truncate=False)

# Save results
artist_authority.limit(100).write.mode("overwrite").csv("outputs/top_100_artists_pagerank.csv", header=True)
artist_authority.write.mode("overwrite").parquet("outputs/artist_pagerank.parquet")
print("✓ PageRank results saved to: artist_pagerank.parquet and top_100_artists_pagerank.csv")

spark.stop()
