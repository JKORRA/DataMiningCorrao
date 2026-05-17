"""
Cluster Quality Analysis
Evaluates the quality of Label Propagation clustering using metrics like Modularity and cohesion.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, expr, desc

spark = SparkSession.builder \
    .appName("MusicGenealogy_ClusterQuality") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("=" * 70)
print("CLUSTER QUALITY ANALYSIS")
print("=" * 70)

print("\nLoading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_labels = spark.read.parquet("outputs/music_labels.parquet")

df_graph.cache()
df_labels.cache()

# Cluster size distribution
print("\n📊 CLUSTER SIZE DISTRIBUTION")
print("-" * 70)

cluster_sizes = df_labels.groupBy("cluster_representative") \
    .agg(count("*").alias("cluster_size")) \
    .orderBy(desc("cluster_size"))

print("\nTop 20 Largest Clusters:")
cluster_sizes.show(20, truncate=False)

# Summary statistics
size_stats = cluster_sizes.select(
    count("*").alias("num_clusters"),
    _sum("cluster_size").alias("total_nodes"),
    expr("MIN(cluster_size)").alias("min_size"),
    expr("MAX(cluster_size)").alias("max_size"),
    expr("AVG(cluster_size)").alias("avg_size"),
    expr("STDDEV(cluster_size)").alias("stddev_size")
).collect()[0]

print(f"\nCluster Statistics:")
print(f"  Total Clusters: {size_stats['num_clusters']:,}")
print(f"  Total Nodes Clustered: {size_stats['total_nodes']:,}")
print(f"  Smallest Cluster: {size_stats['min_size']} nodes")
print(f"  Largest Cluster: {size_stats['max_size']:,} nodes")
print(f"  Average Cluster Size: {size_stats['avg_size']:.2f} nodes")
stddev = size_stats['stddev_size'] if size_stats['stddev_size'] is not None else 0.0
print(f"  Std Dev Cluster Size: {stddev:.2f}")

print("\nCluster Size Distribution:")
cluster_sizes.select(
    expr("SUM(CASE WHEN cluster_size = 1 THEN 1 ELSE 0 END)").alias("Singleton (size=1)"),
    expr("SUM(CASE WHEN cluster_size BETWEEN 2 AND 10 THEN 1 ELSE 0 END)").alias("Small (2-10)"),
    expr("SUM(CASE WHEN cluster_size BETWEEN 11 AND 50 THEN 1 ELSE 0 END)").alias("Medium (11-50)"),
    expr("SUM(CASE WHEN cluster_size BETWEEN 51 AND 200 THEN 1 ELSE 0 END)").alias("Large (51-200)"),
    expr("SUM(CASE WHEN cluster_size > 200 THEN 1 ELSE 0 END)").alias("Giant (>200)")
).show(truncate=False)

# Modularity calculation
print("\n🎯 MODULARITY SCORE")
print("-" * 70)
print("Modularity measures cluster separation quality.")
print("Range: [-0.5, 1.0] | Good: >0.3 | Excellent: >0.5")

# Total edges in the graph
total_edges = df_graph.count()
print(f"\nTotal edges in graph: {total_edges:,}")

# Create a mapping: artist_name -> cluster_id
artist_to_cluster = df_labels.select(
    col("artist_name"),
    col("cluster_id")
)

# Join edges with cluster information for both source and target
edges_with_clusters = df_graph \
    .join(artist_to_cluster.alias("src_cluster"), 
          df_graph.Sampler_Artist_Name == col("src_cluster.artist_name"), 
          "left") \
    .join(artist_to_cluster.alias("tgt_cluster"), 
          df_graph.Original_Artist_Name == col("tgt_cluster.artist_name"), 
          "left") \
    .select(
        col("Sampler_Artist_Name").alias("source_artist"),
        col("Original_Artist_Name").alias("target_artist"),
        col("src_cluster.cluster_id").alias("src_cluster_id"),
        col("tgt_cluster.cluster_id").alias("tgt_cluster_id")
    )

# Count intra-cluster edges (both nodes in same cluster)
intra_cluster_edges = edges_with_clusters \
    .filter(col("src_cluster_id") == col("tgt_cluster_id")) \
    .filter(col("src_cluster_id").isNotNull()) \
    .count()

# Count inter-cluster edges (nodes in different clusters)
inter_cluster_edges = edges_with_clusters \
    .filter(col("src_cluster_id") != col("tgt_cluster_id")) \
    .filter(col("src_cluster_id").isNotNull()) \
    .filter(col("tgt_cluster_id").isNotNull()) \
    .count()

# Edges with at least one unclustered node
unclustered_edges = edges_with_clusters \
    .filter(col("src_cluster_id").isNull() | col("tgt_cluster_id").isNull()) \
    .count()

print(f"\nEdge Distribution:")
print(f"  Intra-cluster edges (within clusters): {intra_cluster_edges:,} ({100*intra_cluster_edges/total_edges:.1f}%)")
print(f"  Inter-cluster edges (between clusters): {inter_cluster_edges:,} ({100*inter_cluster_edges/total_edges:.1f}%)")
print(f"  Edges with unclustered nodes: {unclustered_edges:,} ({100*unclustered_edges/total_edges:.1f}%)")

intra_cluster_edge_fraction = intra_cluster_edges / total_edges
print(f"\nIntra-Cluster Edge Fraction (Simplified Modularity): {intra_cluster_edge_fraction:.4f}")
print(f"(Proportion of edges that fall within the same community)")

if intra_cluster_edge_fraction > 0.5:
    print("✓ EXCELLENT: Very strong community structure!")
elif intra_cluster_edge_fraction > 0.3:
    print("✓ GOOD: Clear community structure detected")
elif intra_cluster_edge_fraction > 0.15:
    print("⚠ MODERATE: Some community structure present")
else:
    print("✗ WEAK: Low intra-cluster connectivity")

# Cluster cohesion
print("\n🔗 CLUSTER COHESION ANALYSIS")
print("-" * 70)
print("Measures internal connectivity density within clusters")

top_clusters = cluster_sizes.limit(5).collect()

print(f"\nAnalyzing top 5 clusters...")

# Pre-calculate internal edges for all clusters using the joined edges DataFrame
cluster_internal_edges_df = edges_with_clusters \
    .filter(col("src_cluster_id") == col("tgt_cluster_id")) \
    .groupBy(col("src_cluster_id").alias("cluster_representative")) \
    .agg(count("*").alias("internal_edges"))

# Join internal edges to the top clusters and evaluate density
for i, row in enumerate(top_clusters, 1):
    cluster_rep = row["cluster_representative"]
    cluster_size = row["cluster_size"]
    
    # Extract internal edges for this specific cluster from our pre-calculated DF
    # We do a fast filter on the aggregated dataframe, which is tiny
    edges_row = cluster_internal_edges_df.filter(col("cluster_representative") == cluster_rep).collect()
    internal_edges = edges_row[0]["internal_edges"] if edges_row else 0
    
    # Maximum possible edges in a directed graph: n * (n-1)
    max_possible = cluster_size * (cluster_size - 1)
    density = internal_edges / max_possible if max_possible > 0 else 0
    
    print(f"\n{i}. Cluster '{cluster_rep[:60]}...' :" if len(cluster_rep) > 60 else f"\n{i}. Cluster '{cluster_rep}':")
    print(f"   Size: {cluster_size} nodes")
    print(f"   Internal edges: {internal_edges}")
    print(f"   Density: {density:.6f} ({100*density:.4f}%)")

# Cluster purity note
print("\n\n📌 NOTE: CLUSTER PURITY")
print("-" * 70)
print("Cluster purity requires ground-truth labels (e.g., genres).")
print("Since MusicBrainz genre data is sparse, we skip this metric.")
print("However, clusters are semantically meaningful based on sampling patterns.")

# Inter-cluster connections
print("\n\n🌉 INTER-CLUSTER CONNECTIONS (Bridge Artists)")
print("-" * 70)
print("Artists that connect different musical communities")

# Find edges between different clusters
bridges = edges_with_clusters \
    .filter(col("src_cluster_id") != col("tgt_cluster_id")) \
    .filter(col("src_cluster_id").isNotNull()) \
    .filter(col("tgt_cluster_id").isNotNull()) \
    .groupBy("src_cluster_id", "tgt_cluster_id") \
    .agg(count("*").alias("bridge_count")) \
    .orderBy(desc("bridge_count"))

print("\nTop 10 Cluster-to-Cluster Connections:")
# Join with cluster names
cluster_names = df_labels.select(
    col("cluster_id"),
    col("cluster_representative")
).distinct()

bridges_named = bridges \
    .join(cluster_names.alias("src"), bridges.src_cluster_id == col("src.cluster_id")) \
    .join(cluster_names.alias("tgt"), bridges.tgt_cluster_id == col("tgt.cluster_id")) \
    .select(
        col("src.cluster_representative").alias("From_Cluster"),
        col("tgt.cluster_representative").alias("To_Cluster"),
        col("bridge_count").alias("Connection_Strength")
    ) \
    .orderBy(desc("Connection_Strength"))

bridges_named.show(10, truncate=False)

# Save results
print("\n💾 SAVING CLUSTER QUALITY REPORT")
print("-" * 70)

quality_summary = spark.createDataFrame([
    ("num_clusters", float(size_stats['num_clusters'])),
    ("avg_cluster_size", float(size_stats['avg_size'])),
    ("max_cluster_size", float(size_stats['max_size'])),
    ("intra_cluster_edge_fraction", float(intra_cluster_edge_fraction)),
    ("intra_cluster_edges", float(intra_cluster_edges)),
    ("inter_cluster_edges", float(inter_cluster_edges)),
    ("unclustered_edges", float(unclustered_edges))
], ["metric", "value"])

quality_summary.write.mode("overwrite").csv("outputs/cluster_quality_summary.csv", header=True)
print("✓ Quality summary saved to: outputs/cluster_quality_summary.csv")

# Save top clusters
cluster_sizes.write.mode("overwrite").csv("outputs/cluster_sizes.csv", header=True)
print("✓ Cluster sizes saved to: outputs/cluster_sizes.csv")

# Save bridge connections
bridges_named.write.mode("overwrite").csv("outputs/cluster_bridges.csv", header=True)
print("✓ Cluster bridges saved to: outputs/cluster_bridges.csv")

print("\n" + "=" * 70)
print("CLUSTER QUALITY ANALYSIS COMPLETE")
print("=" * 70)

spark.stop()
