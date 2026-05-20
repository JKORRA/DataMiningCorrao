from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum, count, when, abs as _abs, max as _max_fn, broadcast

spark = SparkSession.builder \
    .appName("MusicGenealogy_PageRank") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoints")

print("Loading graph...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_graph.cache()
df_graph.count()  # force materialization

# Remove self-loops using Artist Names (GID column is unreliable — contains artist_credit IDs, not MusicBrainz UUIDs)
self_loops = df_graph.filter(col("Sampler_Artist_Name") == col("Original_Artist_Name")).count()
if self_loops > 0:
    print(f"WARNING: Found {self_loops} self-loops! Removing them now...")
    df_graph = df_graph.filter(col("Sampler_Artist_Name") != col("Original_Artist_Name"))
else:
    print("✓ No self-loops detected")

print(f"Graph loaded: {df_graph.count()} edges")

# Shift to Artist-Level PageRank
# Collapse song-level edges into a weighted Artist-to-Artist graph
# Edge direction: Sampler -> Original (authority flows to the sampled/original artist)
print("Collapsing song graph to artist-level graph...")
links = df_graph.groupBy(
    col("Sampler_Artist_Name").alias("src"),
    col("Original_Artist_Name").alias("dst")
).agg(_sum("weight").alias("weight"))

# Calculate out-degree (total weight of outgoing edges, constant across iterations)
out_degrees = links.groupBy("src").agg(_sum("weight").alias("out_wdeg"))

# Initialize all nodes
nodes = links.select("src").union(links.select("dst")).distinct().select(col("src").alias("id"))
N = nodes.count()
print(f"Total Unique Artists in Network: {N}")

ranks = nodes.withColumn("rank", lit(1.0))

# Pre-compute sinks (dangling nodes with no outgoing edges) and cache
sinks = nodes.join(out_degrees, nodes.id == out_degrees.src, "left_anti").cache()

# Cache loop-invariant joined data
links_with_outdeg = links.join(out_degrees, "src").cache()

# PageRank iteration parameters
MAX_ITERATIONS = 50
DAMPING = 0.85
TOLERANCE = 0.01

print(f"Starting PageRank computation (convergence threshold: {TOLERANCE})...")

for i in range(MAX_ITERATIONS):
    print(f"--- Iteration {i+1}/{MAX_ITERATIONS} ---")
    
    old_ranks = ranks
    
    # Calculate mass from dangling nodes (sinks)
    sink_total_row = ranks.join(broadcast(sinks), "id").agg(_sum("rank").alias("sum")).collect()[0]
    sink_total = sink_total_row["sum"] if sink_total_row["sum"] is not None else 0.0
    
    # Base teleport value includes redistributed sink mass
    teleport_per_node = (1.0 - DAMPING) + (DAMPING * sink_total / N)
    
    # Calculate contributions from each source node
    contribs = links_with_outdeg.join(ranks, links_with_outdeg.src == ranks.id) \
                    .select(
                        col("dst"), 
                        (col("rank") * col("weight") / col("out_wdeg")).alias("contribution")
                    )
    
    # Sum contributions received by each destination node
    sum_contribs = contribs.groupBy("dst").agg(_sum("contribution").alias("sum_contrib"))
    
    # Apply PageRank formula
    ranks_calculated = sum_contribs.select(
        col("dst").alias("id"),
        (lit(teleport_per_node) + (lit(DAMPING) * col("sum_contrib"))).alias("rank")
    )
    
    # Handle nodes with no incoming edges (they only get teleport mass)
    ranks = nodes.join(ranks_calculated, "id", "left_outer") \
        .select(
            col("id"),
            when(col("rank").isNull(), lit(teleport_per_node)).otherwise(col("rank")).alias("rank")
        )
    
    ranks = ranks.checkpoint()
    
    # Check convergence
    diff_check = ranks.join(broadcast(old_ranks.select(col("id"), col("rank").alias("old_rank"))), "id") \
        .select(_abs(col("rank") - col("old_rank")).alias("abs_diff"))
    
    max_diff = diff_check.agg(_max_fn("abs_diff").alias("max_diff")).collect()[0]["max_diff"]
    
    print(f"    Max rank change: {max_diff:.6f}")
    
    if max_diff < TOLERANCE:
        print(f"✓ Convergence reached after {i+1} iterations!")
        break

# Results: id column already contains artist names
print("Preparing final results...")

artist_authority = ranks.select(
    col("id").alias("artist"), 
    col("rank").alias("authority_score")
).orderBy(col("authority_score").desc())

print("\n--- TOP 20 ARTISTS BY AUTHORITY (Artist-Level PageRank) ---")
artist_authority.show(20, truncate=False)

# Save results
artist_authority.limit(100).write.mode("overwrite").csv("outputs/top_100_artists_pagerank.csv", header=True)
artist_authority.write.mode("overwrite").parquet("outputs/artist_pagerank.parquet")
print("✓ PageRank results saved to: artist_pagerank.parquet and top_100_artists_pagerank.csv")

spark.stop()
