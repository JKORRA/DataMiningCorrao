"""
Music Genealogy Network Visualizations
Merged Hub Analysis + Top Bridges figure (Fig 2).
"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, desc, sqrt as _sqrt
import os
import warnings

warnings.filterwarnings("ignore")

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "IPAGothic", "IPAMincho", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 11
plt.rcParams["axes.labelsize"] = 12
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["legend.fontsize"] = 10

output_dir = "figures/report_figures"
os.makedirs(output_dir, exist_ok=True)


def is_readable(name):
    if not isinstance(name, str) or name in ("[unknown]", "[no artist]", ""):
        return False
    return name.isprintable()


print("=" * 80)
print("FIGURE 2: HUB ANALYSIS + BRIDGES (MERGED)")
print("=" * 80)

spark = (
    SparkSession.builder.appName("MusicGenealogy_Fig2")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\nLoading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")
df_labels = spark.read.parquet("outputs/music_labels.parquet")

in_degree = (
    df_graph.filter(col("Original_Artist_Name") != "[unknown]")
    .groupBy("Original_Artist_Name")
    .agg(count("*").alias("in_degree"))
    .withColumnRenamed("Original_Artist_Name", "artist")
)

out_degree = (
    df_graph.filter(col("Sampler_Artist_Name") != "[unknown]")
    .groupBy("Sampler_Artist_Name")
    .agg(count("*").alias("out_degree"))
    .withColumnRenamed("Sampler_Artist_Name", "artist")
)

degree_df = in_degree.join(out_degree, "artist", "outer").na.fill(0).toPandas()
degree_df = degree_df[degree_df["artist"].apply(is_readable)]

df_pr = df_pagerank.toPandas()
degree_df = degree_df.merge(df_pr, on="artist", how="left")
degree_df["authority_score"] = degree_df["authority_score"].fillna(0.1)

artist_cluster_pd = df_labels.select("artist_name", "cluster_representative").dropDuplicates(["artist_name"]).toPandas()
cluster_map = dict(zip(artist_cluster_pd["artist_name"], artist_cluster_pd["cluster_representative"]))
degree_df["cluster"] = degree_df["artist"].map(cluster_map).fillna("Unknown")

spark.stop()

# --- Left Panel: Hub Scatter ---
in_deg_log = degree_df["in_degree"] + 1
out_deg_log = degree_df["out_degree"] + 1

median_in = in_deg_log.median()
median_out = out_deg_log.median()

top_volume = degree_df.sort_values("in_degree", ascending=False).head(15)
top_bridges = degree_df.copy()
top_bridges["bridge_score"] = np.sqrt(top_bridges["in_degree"] * top_bridges["out_degree"])
top_bridges = top_bridges.sort_values("bridge_score", ascending=False).head(15)

unique_clusters = [c for c in degree_df["cluster"].unique() if c != "Unknown"]
unique_clusters = sorted(unique_clusters, key=lambda c: len(degree_df[degree_df["cluster"] == c]), reverse=True)
cmap = plt.colormaps["tab10"].resampled(max(len(unique_clusters), 1))
cluster_color_map = {c: cmap(i) for i, c in enumerate(unique_clusters)}
cluster_color_map["Unknown"] = (0.6, 0.6, 0.6, 1)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 9))

scatter_colors = [cluster_color_map.get(c, (0.6, 0.6, 0.6, 1)) for c in degree_df["cluster"]]

ax1.scatter(
    out_deg_log, in_deg_log,
    s=25, alpha=0.3, c=scatter_colors, edgecolors="none", zorder=2,
)

top_15_df = degree_df[degree_df["artist"].isin(top_volume["artist"].tolist())]
ax1.scatter(
    top_15_df["out_degree"] + 1, top_15_df["in_degree"] + 1,
    s=150, c="#E74C3C", edgecolors="#C0392B", linewidths=2, zorder=5, label="Top 15 by Volume",
)

ax1.axhline(y=median_in, color="gray", linestyle="--", alpha=0.5, linewidth=1.2)
ax1.axvline(x=median_out, color="gray", linestyle="--", alpha=0.5, linewidth=1.2)

# Label bridges
bridges_to_label = top_bridges.head(8)
label_y_positions = np.linspace(
    max(in_deg_log) * 0.85, min(bridges_to_label["in_degree"] + 1) * 0.95, len(bridges_to_label)
)
for idx, (_, row) in enumerate(bridges_to_label.iterrows()):
    label = row["artist"] if len(row["artist"]) <= 20 else row["artist"][:18] + "..."
    ax1.annotate(
        label,
        xy=(row["out_degree"] + 1, row["in_degree"] + 1),
        xytext=(max(out_deg_log) * 1.02, label_y_positions[idx]),
        textcoords="data",
        fontsize=8, fontweight="bold", ha="left", va="center",
        bbox=dict(boxstyle="round,pad=0.2", facecolor="#FFFACD", edgecolor="#E74C3C", alpha=0.85),
        arrowprops=dict(arrowstyle="-|>", color="#E74C3C", lw=0.8, alpha=0.6),
    )

ax1.set_xlabel("Out-Degree (Samples Used)", fontsize=12, fontweight="bold")
ax1.set_ylabel("In-Degree (Times Sampled)", fontsize=12, fontweight="bold")
ax1.set_title("Artist Classification: Hub Analysis\n(Color = Cluster, Red = Top 15 by Volume)", fontsize=13, fontweight="bold", pad=15)
ax1.set_xscale("log")
ax1.set_yscale("log")
ax1.grid(True, alpha=0.3, linestyle=":")
ax1.legend(loc="lower right", fontsize=9, framealpha=0.9)

# Quadrant labels
ax1.text(0.98, 0.98, "BRIDGES", transform=ax1.transAxes, ha="right", va="top",
         fontsize=10, fontweight="bold", bbox=dict(boxstyle="round", facecolor="#E8F5E9", alpha=0.8))
ax1.text(0.05, 0.95, "AUTHORITIES", transform=ax1.transAxes, ha="left", va="top",
         fontsize=10, fontweight="bold", bbox=dict(boxstyle="round", facecolor="#FFF3E0", alpha=0.8))
ax1.text(0.95, 0.05, "HEAVY\nSAMPLERS", transform=ax1.transAxes, ha="right", va="bottom",
         fontsize=10, fontweight="bold", bbox=dict(boxstyle="round", facecolor="#E3F2FD", alpha=0.8))
ax1.text(0.05, 0.05, "PERIPHERAL", transform=ax1.transAxes, ha="left", va="bottom",
         fontsize=10, fontweight="bold", bbox=dict(boxstyle="round", facecolor="#F5F5F5", alpha=0.8))

# --- Right Panel: Top Bridges bar chart ---
bar_colors = [cluster_color_map.get(c, (0.6, 0.6, 0.6, 1)) for c in top_bridges["cluster"]]
y_pos = np.arange(len(top_bridges))

ax2.barh(y_pos, top_bridges["bridge_score"], color=bar_colors, edgecolor="white", height=0.7)
ax2.set_yticks(y_pos)
ax2.set_yticklabels(top_bridges["artist"], fontweight="bold", fontsize=9)
ax2.invert_yaxis()

for i, (_, row) in enumerate(top_bridges.iterrows()):
    ax2.text(row["bridge_score"] + 1, i, f"In:{int(row['in_degree'])} Out:{int(row['out_degree'])}",
             va="center", fontsize=7, color="#555")

ax2.set_xlabel("Bridge Score (√(In × Out))", fontsize=12, fontweight="bold")
ax2.set_title("Top 15 Evolutionary Bridges\n(in-degree × out-degree balance)", fontsize=13, fontweight="bold", pad=15)
ax2.grid(axis="x", alpha=0.3)

# Cluster legend for both panels
legend_patches = []
shown = 0
for clus in unique_clusters:
    if shown >= 8:
        break
    count_in_graph = len(degree_df[degree_df["cluster"] == clus])
    legend_patches.append(mpatches.Patch(color=cluster_color_map[clus], label=f"{clus} ({count_in_graph})"))
    shown += 1
fig.legend(handles=legend_patches, loc="lower center", ncol=min(shown, 4),
           framealpha=0.9, fontsize=8, title="Cluster (artist count)", title_fontsize=9)

plt.tight_layout(rect=[0, 0.06, 1, 1])

png_path = f"{output_dir}/fig2_hub_bridges.png"
pdf_path = f"{output_dir}/fig2_hub_bridges.pdf"
plt.savefig(png_path, dpi=300, bbox_inches="tight")
plt.savefig(pdf_path, bbox_inches="tight")
print(f"  Saved: {png_path}")
print(f"  Saved: {pdf_path}")
plt.close()

import shutil
report_img_dir = "../report/Immagini"
if os.path.exists(report_img_dir):
    shutil.copy2(png_path, os.path.join(report_img_dir, "fig2_hub_bridges.png"))
    shutil.copy2(pdf_path, os.path.join(report_img_dir, "fig2_hub_bridges.pdf"))
    print(f"  Copied to {report_img_dir}/")

print("Done!")
