from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as _max, count, first, lit, greatest

spark = SparkSession.builder \
    .appName("MusicGenealogy_Clustering") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoints_clustering")

print("Loading graph for clustering...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")

# Verify self-loops were removed
self_loops = df_graph.filter(col("Sampler_Artist_Name") == col("Original_Artist_Name")).count()
if self_loops > 0:
    print(f"WARNING: Found {self_loops} self-loops! Removing them now...")
    df_graph = df_graph.filter(col("Sampler_Artist_Name") != col("Original_Artist_Name"))
else:
    print("✓ No self-loops detected")

print(f"Graph loaded: {df_graph.count()} edges")

# Create edges: child samples parent
# Clustering groups artists who sample the same sources (shared influences)
edges = df_graph.select(
    col("source_song_id").alias("child"), 
    col("target_song_id").alias("parent")
).distinct()

# Get all unique nodes
nodes = edges.select("child").union(edges.select("parent")).distinct()

# Initialize: each node starts with its own ID as label
labels = nodes.select(col("child").alias("id"), col("child").alias("label"))

# Label Propagation Algorithm
# Each node adopts the label of its most influential neighbor
ITERATIONS = 6 
print(f"Starting Label Propagation ({ITERATIONS} iterations)...")

for i in range(ITERATIONS):
    print(f"--- Iteration {i+1}/{ITERATIONS} ---")
    
    # Each child receives its parent's label
    propagation = edges.join(labels, edges.parent == labels.id) \
                       .select(col("child"), col("label").alias("parent_label"))
    
    # Each node considers both its current label and proposed labels
    current_state = labels.alias("l").join(propagation.alias("p"), col("l.id") == col("p.child"), "left_outer") \
        .select(
            col("l.id"),
            col("l.label").alias("old_label"),
            col("p.parent_label").alias("new_proposal")
        )
    
    # Adopt the maximum label among all proposals
    labels = current_state.groupBy("id") \
        .agg(_max(col("new_proposal")).alias("max_proposal"), first("old_label").alias("old")) \
        .select(
            col("id"),
            greatest(col("max_proposal"), col("old")).alias("label") 
        ).checkpoint()

# Analyze resulting clusters
print("Clustering complete. Aggregating results...")

song_artist_map = df_graph.select(
    col("target_song_id").alias("song_id"), 
    col("Original_Artist_Name").alias("artist_name")
).distinct()

final_clusters = labels.join(song_artist_map, labels.label == song_artist_map.song_id) \
    .select(
        col("id").alias("song_id"),
        col("label").alias("cluster_id"),
        col("artist_name").alias("cluster_representative")
    )

cluster_sizes = final_clusters.groupBy("cluster_representative") \
    .agg(count("*").alias("cluster_size")) \
    .orderBy(col("cluster_size").desc())

print("\n--- TOP 20 LARGEST MUSICAL COMMUNITIES ---")
print("These artists define the largest 'genealogical families' in music:")
cluster_sizes.show(20, truncate=False)

# Save results
print("Saving results...")
final_clusters.write.mode("overwrite").parquet("outputs/music_labels.parquet")
cluster_sizes.limit(100).write.mode("overwrite").csv("outputs/music_clusters.csv")
print("✓ Clusters saved to: music_labels.parquet and music_clusters.csv")

spark.stop()
