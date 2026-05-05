from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as _max, count, first, lit, when, rand, broadcast, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("MusicGenealogy_Clustering") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoints_clustering")

print("Loading graph for clustering...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")

# Remove self-loops using Artist Names (GID column is unreliable)
self_loops = df_graph.filter(col("Sampler_Artist_Name") == col("Original_Artist_Name")).count()
if self_loops > 0:
    print(f"WARNING: Found {self_loops} self-loops! Removing them now...")
    df_graph = df_graph.filter(col("Sampler_Artist_Name") != col("Original_Artist_Name"))
else:
    print("✓ No self-loops detected")

print(f"Graph loaded: {df_graph.count()} edges")

# Create edges: child samples parent (Artist-Level clustering)
edges = df_graph.select(
    col("Sampler_Artist_Name").alias("child"), 
    col("Original_Artist_Name").alias("parent")
).distinct()

# Get all unique nodes
nodes = edges.select("child").union(edges.select("parent")).distinct()

# Initialize: each node starts with its own ID as label
labels = nodes.select(col("child").alias("id"), col("child").alias("label"))

# Label Propagation Algorithm (LPA)
# Each node adopts the majority label of its neighbors (Random tie-breaking)
ITERATIONS = 6 
print(f"Starting Label Propagation ({ITERATIONS} iterations)...")

for i in range(ITERATIONS):
    print(f"--- Iteration {i+1}/{ITERATIONS} ---")
    
    # Each child receives its parent's label
    propagation = edges.join(labels, edges.parent == labels.id) \
                       .select(col("child"), col("label").alias("parent_label"))
    
    # Count frequency of each proposed label
    prop_counts = propagation.groupBy("child", "parent_label").count()
    
    # Find max count per child
    max_counts = prop_counts.groupBy("child").agg(_max("count").alias("max_count"))
    
    # Join back to get all labels with the maximum count (the contenders)
    contenders = prop_counts.join(max_counts, ["child"]) \
                            .filter(col("count") == col("max_count")) \
                            .select("child", "parent_label")
    
    # Random tie-breaking: use Window functions for distributed random selection
    w = Window.partitionBy("child").orderBy("rand_val")
    new_proposals = contenders.withColumn("rand_val", rand()) \
                              .withColumn("rn", row_number().over(w)) \
                              .filter(col("rn") == 1) \
                              .select("child", col("parent_label").alias("new_proposal"))
    
    # Update state: adopt new proposal if it exists, otherwise keep old label
    current_state = labels.alias("l").join(new_proposals.alias("p"), col("l.id") == col("p.child"), "left_outer")
    
    labels = current_state.select(
        col("l.id").alias("id"),
        when(col("p.new_proposal").isNull(), col("l.label")) \
            .otherwise(col("p.new_proposal")).alias("label")
    )
    
    if (i + 1) % 3 == 0:
        labels = labels.checkpoint()
    else:
        labels = labels.cache()

# Analyze resulting clusters
print("Clustering complete. Aggregating results...")

# The label IS the cluster representative (an artist name)
final_clusters = labels.select(
    col("id").alias("artist_name"),
    col("label").alias("cluster_id"),
    col("label").alias("cluster_representative")
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
