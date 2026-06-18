"""
Advanced Data Mining Experiments: Authority Context

This script generates Figure 3: Authority Context (Internal vs External Influence).
It analyzes the top 15 authorities (by PageRank) to determine if their influence
is "mainstream" (sampled by many different communities) or "niche/internal" 
(sampled primarily by artists within their own specific community).

The pipeline performs the following steps:
1. Loads the music graph, PageRank scores, and community labels.
2. Identifies the top 15 authorities.
3. Retrieves all incoming sampling edges for these 15 artists.
4. Joins the edges with community labels to determine if the sampler and original artist belong to the same community.
5. Calculates the percentage of internal vs external incoming samples.
6. Generates a stacked bar chart visualizing these proportions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum as _sum, when
import matplotlib.pyplot as plt
import numpy as np
import os
import shutil
import warnings

warnings.filterwarnings("ignore")

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 11

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "IPAGothic", "IPAMincho", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

output_dir = "figures/report_figures"
os.makedirs(output_dir, exist_ok=True)


def is_readable(name):
    if not isinstance(name, str) or name in ("[unknown]", "[no artist]", ""):
        return False
    return name.isprintable()


print("=" * 80)
print("FIGURE 3: AUTHORITY CONTEXT (Internal vs External Influence)")
print("=" * 80)

spark = (
    SparkSession.builder.appName("AdvancedExperiments")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\nLoading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")
try:
    df_labels = spark.read.parquet("outputs/music_labels.parquet")
except:
    print("Warning: music_labels.parquet not found. Skipping.")
    spark.stop()
    exit(0)

print("Analyzing internal vs external authority...")

# Get top 15 authorities
top_auth_pd = df_pagerank.orderBy(desc("authority_score")).toPandas()
top_auth_pd = top_auth_pd[top_auth_pd["artist"].apply(is_readable)].head(15)
top_15_names = top_auth_pd["artist"].tolist()

# Map each artist to their cluster
artist_clusters = df_labels.select("artist_name", "cluster_representative").dropDuplicates(["artist_name"])

# Get all incoming edges for the top 15 authorities
edges = df_graph.filter(col("Original_Artist_Name").isin(top_15_names)).select(
    "Sampler_Artist_Name", "Original_Artist_Name"
)

# Join to get cluster of Original Artist
edges = edges.join(
    artist_clusters, edges.Original_Artist_Name == artist_clusters.artist_name, "left"
).withColumnRenamed("cluster_representative", "original_cluster").drop("artist_name")

# Join to get cluster of Sampler Artist
edges = edges.join(
    artist_clusters, edges.Sampler_Artist_Name == artist_clusters.artist_name, "left"
).withColumnRenamed("cluster_representative", "sampler_cluster").drop("artist_name")

# Categorize each incoming edge as Internal (same community) or External
edges = edges.withColumn(
    "is_internal",
    when(col("original_cluster") == col("sampler_cluster"), 1).otherwise(0),
)

# Aggregate internal vs total samples per artist
stats = (
    edges.groupBy("Original_Artist_Name")
    .agg(count("*").alias("total_samples"), _sum("is_internal").alias("internal_samples"))
    .toPandas()
)

stats["external_samples"] = stats["total_samples"] - stats["internal_samples"]
stats["internal_pct"] = (stats["internal_samples"] / stats["total_samples"]) * 100
stats["external_pct"] = (stats["external_samples"] / stats["total_samples"]) * 100

stats.set_index("Original_Artist_Name", inplace=True)
stats = stats.reindex(top_15_names)

fig, ax = plt.subplots(figsize=(14, 8))
y_pos = np.arange(len(top_15_names))

p1 = ax.barh(
    y_pos, stats["internal_pct"],
    color="#E74C3C", edgecolor="white", height=0.7,
    label="Internal Influence (Same Community)",
)
p2 = ax.barh(
    y_pos, stats["external_pct"],
    left=stats["internal_pct"], color="#3498DB", edgecolor="white", height=0.7,
    label="External Influence (Other Communities)",
)

ax.set_yticks(y_pos)
ax.set_yticklabels(top_15_names, fontweight="bold")
ax.invert_yaxis()

for i in range(len(top_15_names)):
    int_pct = stats.iloc[i]["internal_pct"]
    ext_pct = stats.iloc[i]["external_pct"]
    total = int(stats.iloc[i]["total_samples"])
    label = f"  n={total}"

    if int_pct > 15:
        ax.text(int_pct / 2, i, f"{int_pct:.0f}%{label}", va="center", ha="center", color="white", fontweight="bold", fontsize=9)
    else:
        ax.text(int_pct + 1, i, f"{int_pct:.0f}%", va="center", color="#E74C3C", fontweight="bold", fontsize=8)

    if ext_pct > 15:
        ax.text(int_pct + ext_pct / 2, i, f"{ext_pct:.0f}%", va="center", ha="center", color="white", fontweight="bold", fontsize=9)

ax.axvline(x=50, color="gray", linestyle="--", alpha=0.7)

ax.set_xlabel("Percentage of Incoming Samples", fontsize=12, fontweight="bold")
ax.set_title(
    "Authority Context: Internal vs External Influence\nAre top authorities mainstream icons or niche community leaders?",
    fontsize=15, fontweight="bold", pad=35,
)
ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.01), ncol=2, framealpha=1)

plt.tight_layout()

png_path = f"{output_dir}/fig3_authority_context.png"
pdf_path = f"{output_dir}/fig3_authority_context.pdf"
plt.savefig(png_path, bbox_inches="tight")
plt.savefig(pdf_path, bbox_inches="tight")
print(f"  Saved: {png_path}")
print(f"  Saved: {pdf_path}")
plt.close()

report_img_dir = "../report/Immagini"
if os.path.exists(report_img_dir):
    shutil.copy2(png_path, os.path.join(report_img_dir, "fig3_authority_context.png"))
    shutil.copy2(pdf_path, os.path.join(report_img_dir, "fig3_authority_context.pdf"))
    print(f"  Copied to {report_img_dir}/")

spark.stop()
print("Done!")
