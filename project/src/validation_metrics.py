"""
Music Graph Validation Metrics
Calculates comprehensive statistics to validate data quality and understand network structure.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, min as _min, max as _max, expr

spark = SparkSession.builder \
    .appName("MusicGenealogy_Validation") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("=" * 70)
print("MUSIC GRAPH VALIDATION METRICS")
print("=" * 70)

df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_graph.cache()

# Basic graph statistics
print("\n📊 BASIC GRAPH STATISTICS")
print("-" * 70)

total_edges = df_graph.count()
print(f"Total Sampling Events (Edges): {total_edges:,}")

total_songs = df_graph.select("source_song_id").union(
    df_graph.select("target_song_id")
).distinct().count()
print(f"Unique Songs (Nodes): {total_songs:,}")

total_artists = df_graph.select("Original_Artist_Name").union(
    df_graph.select("Sampler_Artist_Name")
).distinct().count()
print(f"Unique Artists: {total_artists:,}")

avg_edges_per_artist = total_edges / total_artists
print(f"Average Edges per Artist: {avg_edges_per_artist:.2f}")

density = total_edges / (total_songs * (total_songs - 1))
print(f"Graph Density: {density:.6f} (Very sparse - typical for real networks)")

# In-degree distribution
print("\n📈 IN-DEGREE DISTRIBUTION (Times Sampled)")
print("-" * 70)

in_degree = df_graph.groupBy("Original_Artist_Name") \
    .agg(count("*").alias("in_degree"))

in_stats = in_degree.select(
    _min("in_degree").alias("min"),
    _max("in_degree").alias("max"),
    mean("in_degree").alias("mean"),
    stddev("in_degree").alias("stddev")
).collect()[0]

print(f"Min In-Degree: {in_stats['min']}")
print(f"Max In-Degree: {in_stats['max']}")
print(f"Mean In-Degree: {in_stats['mean']:.2f}")
print(f"Std Dev In-Degree: {in_stats['stddev']:.2f}")

print("\nIn-Degree Distribution:")
in_degree.select(
    (col("in_degree") >= 1).alias("1+"),
    (col("in_degree") >= 5).alias("5+"),
    (col("in_degree") >= 10).alias("10+"),
    (col("in_degree") >= 50).alias("50+"),
    (col("in_degree") >= 100).alias("100+")
).select(
    expr("SUM(CASE WHEN `1+` THEN 1 ELSE 0 END)").alias("Sampled 1+ times"),
    expr("SUM(CASE WHEN `5+` THEN 1 ELSE 0 END)").alias("Sampled 5+ times"),
    expr("SUM(CASE WHEN `10+` THEN 1 ELSE 0 END)").alias("Sampled 10+ times"),
    expr("SUM(CASE WHEN `50+` THEN 1 ELSE 0 END)").alias("Sampled 50+ times"),
    expr("SUM(CASE WHEN `100+` THEN 1 ELSE 0 END)").alias("Sampled 100+ times")
).show(truncate=False)

# Out-degree distribution
print("\n📉 OUT-DEGREE DISTRIBUTION (Samples Used)")
print("-" * 70)

out_degree = df_graph.groupBy("Sampler_Artist_Name") \
    .agg(count("*").alias("out_degree"))

out_stats = out_degree.select(
    _min("out_degree").alias("min"),
    _max("out_degree").alias("max"),
    mean("out_degree").alias("mean"),
    stddev("out_degree").alias("stddev")
).collect()[0]

print(f"Min Out-Degree: {out_stats['min']}")
print(f"Max Out-Degree: {out_stats['max']}")
print(f"Mean Out-Degree: {out_stats['mean']:.2f}")
print(f"Std Dev Out-Degree: {out_stats['stddev']:.2f}")
print(f"Max Out-Degree: {out_stats['max']}")

print("\nOut-Degree Distribution:")
out_degree.select(
    (col("out_degree") >= 1).alias("1+"),
    (col("out_degree") >= 5).alias("5+"),
    (col("out_degree") >= 10).alias("10+"),
    (col("out_degree") >= 50).alias("50+"),
    (col("out_degree") >= 100).alias("100+")
).select(
    expr("SUM(CASE WHEN `1+` THEN 1 ELSE 0 END)").alias("Used 1+ samples"),
    expr("SUM(CASE WHEN `5+` THEN 1 ELSE 0 END)").alias("Used 5+ samples"),
    expr("SUM(CASE WHEN `10+` THEN 1 ELSE 0 END)").alias("Used 10+ samples"),
    expr("SUM(CASE WHEN `50+` THEN 1 ELSE 0 END)").alias("Used 50+ samples"),
    expr("SUM(CASE WHEN `100+` THEN 1 ELSE 0 END)").alias("Used 100+ samples")
).show(truncate=False)

# Power law analysis
print("\n⚡ POWER LAW ANALYSIS")
print("-" * 70)
print("Checking if the network follows power-law distribution (scale-free network)")

# Top 1% of nodes should have a disproportionate share of connections
top_1_percent = max(1, int(total_artists * 0.01))

in_top_1pct = in_degree.orderBy(col("in_degree").desc()).limit(top_1_percent)
in_top_sum = in_top_1pct.agg(count("*").alias("count"), expr("SUM(in_degree)").alias("sum")).collect()[0]

print(f"\nIn-Degree (Sampled):")
print(f"  Top 1% of artists ({in_top_sum['count']} artists) account for:")
print(f"  {in_top_sum['sum']:,} / {total_edges:,} = {100*in_top_sum['sum']/total_edges:.1f}% of all sampling events")

out_top_1pct = out_degree.orderBy(col("out_degree").desc()).limit(top_1_percent)
out_top_sum = out_top_1pct.agg(count("*").alias("count"), expr("SUM(out_degree)").alias("sum")).collect()[0]

print(f"\nOut-Degree (Samplers):")
print(f"  Top 1% of artists ({out_top_sum['count']} artists) account for:")
print(f"  {out_top_sum['sum']:,} / {total_edges:,} = {100*out_top_sum['sum']/total_edges:.1f}% of all sampling events")

if (100*in_top_sum['sum']/total_edges) > 20:
    print("\n✓ Network exhibits power-law characteristics (scale-free)")
    print("  → A few 'hub' artists dominate the influence structure")
else:
    print("\n✗ Network does NOT exhibit strong power-law characteristics")

# Data quality checks
print("\n🔍 DATA QUALITY CHECKS")
print("-" * 70)

# Check for self-loops
self_loops = df_graph.filter(
    col("source_song_id") == col("target_song_id")
).count()
print(f"Self-loops (artists sampling themselves): {self_loops}")

# Check for null values
null_check = df_graph.select(
    expr("SUM(CASE WHEN source_song_id IS NULL THEN 1 ELSE 0 END)").alias("null_source"),
    expr("SUM(CASE WHEN target_song_id IS NULL THEN 1 ELSE 0 END)").alias("null_target"),
    expr("SUM(CASE WHEN Sampler_Artist_Name IS NULL THEN 1 ELSE 0 END)").alias("null_sampler"),
    expr("SUM(CASE WHEN Original_Artist_Name IS NULL THEN 1 ELSE 0 END)").alias("null_original")
).collect()[0]

print(f"Null source_song_id: {null_check['null_source']}")
print(f"Null target_song_id: {null_check['null_target']}")
print(f"Null Sampler_Artist_Name: {null_check['null_sampler']}")
print(f"Null Original_Artist_Name: {null_check['null_original']}")

# Duplicate edges
duplicate_edges = df_graph.groupBy("source_song_id", "target_song_id") \
    .agg(count("*").alias("cnt")) \
    .filter(col("cnt") > 1) \
    .count()
print(f"Duplicate edges: {duplicate_edges}")

# Save summary
print("\n💾 SAVING VALIDATION REPORT")
print("-" * 70)

summary_data = [
    ("total_edges", float(total_edges)),
    ("total_songs", float(total_songs)),
    ("total_artists", float(total_artists)),
    ("avg_edges_per_artist", float(avg_edges_per_artist)),
    ("graph_density", float(density)),
    ("in_degree_mean", float(in_stats['mean'])),
    ("in_degree_stddev", float(in_stats['stddev'])),
    ("in_degree_max", float(in_stats['max'])),
    ("out_degree_mean", float(out_stats['mean'])),
    ("out_degree_stddev", float(out_stats['stddev'])),
    ("out_degree_max", float(out_stats['max'])),
    ("self_loops", float(self_loops)),
    ("duplicate_edges", float(duplicate_edges))
]

summary_df = spark.createDataFrame(summary_data, ["metric", "value"])
summary_df.write.mode("overwrite").csv("outputs/validation_summary.csv", header=True)
print("✓ Validation summary saved to: ../outputs/validation_summary.csv")

print("\n" + "=" * 70)
print("VALIDATION COMPLETE")
print("=" * 70)

spark.stop()
