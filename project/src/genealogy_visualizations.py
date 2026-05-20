"""
Music Genealogy Network Visualizations
Creates publication-quality network visualizations for the music sampling network.
Filters out [unknown] artists and unreadable characters for clean presentation.
"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "IPAGothic", "IPAMincho", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, desc
import re
import warnings

warnings.filterwarnings("ignore")

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 11
plt.rcParams["axes.labelsize"] = 12
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["legend.fontsize"] = 10

print("=" * 80)
print("MUSIC GENEALOGY NETWORK VISUALIZATIONS")
print("=" * 80)


# --- Helpers ---
# Detect names that contain non-Latin characters (CJK, etc.) which render as boxes in PDF
def is_readable(name):
    if not isinstance(name, str) or name in ("[unknown]", "[no artist]", ""):
        return False
    return name.isprintable()


MIN_EDGE_WEIGHT = 1
TOP_N_ARTISTS = 15

spark = (
    SparkSession.builder.appName("MusicGenealogy_CleanViz")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\n[1/6] Loading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")
print(f"Graph loaded: {df_graph.count()} edges")

import os

output_dir = "figures/report_figures"
os.makedirs(output_dir, exist_ok=True)

# =============================================================================
# FIGURE 1: TOP 15 ARTISTS SAMPLING NETWORK
# Use top artists by IN-DEGREE (volume) for meaningful, recognizable names
# =============================================================================
print("=" * 80)
print(f"[FIGURE 1] Top {TOP_N_ARTISTS} Artists by Volume - Clean Sampling Network")
print("=" * 80)

# Get top N by VOLUME (in-degree), filtering out unreadable names
all_artists_volume = (
    df_graph.filter(col("Original_Artist_Name") != "[unknown]")
    .groupBy("Original_Artist_Name")
    .agg(count("*").alias("in_degree"))
    .orderBy(desc("in_degree"))
    .toPandas()
)

# Filter for readable names
readable_volume = all_artists_volume[
    all_artists_volume["Original_Artist_Name"].apply(is_readable)
]
top_artists = readable_volume.head(TOP_N_ARTISTS)["Original_Artist_Name"].tolist()
print(f"Selected top {TOP_N_ARTISTS} artists: {', '.join(top_artists[:5])}...")

# Get edges for these top artists - include exterior nodes with strong connections (weight >= 5) to make it richer
artist_edges = (
    df_graph.filter(
        (col("Original_Artist_Name").isin(top_artists))
        & (col("Sampler_Artist_Name").isin(top_artists))
    )
    .groupBy("Sampler_Artist_Name", "Original_Artist_Name")
    .agg(count("*").alias("weight"))
    .toPandas()
)

print(
    f"Found {len(artist_edges)} strong sampling relationships involving the Top {TOP_N_ARTISTS}"
)

G_top = nx.DiGraph()
# Add ALL top artists as nodes (even if no inter-edges)
for a in top_artists:
    G_top.add_node(a)
for _, row in artist_edges.iterrows():
    G_top.add_edge(
        row["Sampler_Artist_Name"], row["Original_Artist_Name"], weight=row["weight"]
    )

# Get volume for node sizing
volume_dict = dict(
    zip(readable_volume["Original_Artist_Name"], readable_volume["in_degree"])
)
node_sizes = [volume_dict.get(node, 5) * 12 for node in G_top.nodes()]
node_colors = [
    "#E74C3C" if node in top_artists else "#3498DB" for node in G_top.nodes()
]

pos = nx.spring_layout(G_top, k=3.5, iterations=300, seed=42)
fig, ax = plt.subplots(figsize=(18, 14))

edges = G_top.edges()
if edges:
    weights = [G_top[u][v]["weight"] for u, v in edges]
    max_w = max(weights) if weights else 1
    nx.draw_networkx_edges(
        G_top,
        pos,
        edge_color="#7f8c8d",
        width=[(w / max_w) * 6 + 1 for w in weights],
        alpha=0.6,
        arrows=True,
        arrowsize=25,
        arrowstyle="-|>",
        connectionstyle="arc3,rad=0.15",
        node_size=node_sizes,
        ax=ax,
    )

nx.draw_networkx_nodes(
    G_top,
    pos,
    node_size=node_sizes,
    node_color=node_colors,
    edgecolors="white",
    linewidths=2.5,
    ax=ax,
)

# Label all 15 artists
for i, node in enumerate(top_artists):
    if node in pos:
        x, y = pos[node]
        ax.text(
            x,
            y,
            node,
            fontsize=13,
            fontweight="bold",
            ha="center",
            va="center",
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor="white",
                edgecolor="none",
                alpha=0.8,
            ),
        )

ax.set_title(
    f"Top {TOP_N_ARTISTS} Most-Sampled Artists: Network Connections\n(Arrow: Sampler → Original, Node Size ∝ Times Sampled, Edges with weight ≥ {MIN_EDGE_WEIGHT})",
    fontsize=15,
    fontweight="bold",
    pad=25,
)
ax.axis("off")

legend_elements = [
    Line2D(
        [0],
        [0],
        color="#666666",
        linewidth=3,
        label=f"Sampling relationship (≥{MIN_EDGE_WEIGHT} times)",
    ),
    Line2D(
        [0],
        [0],
        marker="o",
        color="w",
        markerfacecolor="#4A90E2",
        markersize=14,
        label="Artist (Size ∝ Times Sampled)",
        markeredgecolor="#2C5AA0",
        markeredgewidth=2,
    ),
]
ax.legend(handles=legend_elements, loc="upper right", framealpha=0.95, fontsize=11)
plt.tight_layout()
plt.savefig(
    f"{output_dir}/fig1_top15_sampling_network.png", dpi=300, bbox_inches="tight"
)
plt.savefig(f"{output_dir}/fig1_top15_sampling_network.pdf", bbox_inches="tight")
print(f"✓ Saved: fig1_top15_sampling_network.png/pdf\n")
plt.close()



# =============================================================================
# FIGURE 6: HUB ANALYSIS - filtered
# =============================================================================
print("=" * 80)
print("[FIGURE 6] Hub Analysis - In-Degree vs Out-Degree")
print("=" * 80)

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

# Filter for readable names
degree_df = degree_df[degree_df["artist"].apply(is_readable)]

# Get top 15 by volume for labeling (more recognizable than PageRank top 15)
top_15_vol = readable_volume.head(15)["Original_Artist_Name"].tolist()

# Get pagerank for colors
df_pr = df_pagerank.toPandas()
degree_df = degree_df.merge(df_pr, on="artist", how="left")
degree_df["authority_score"] = degree_df["authority_score"].fillna(0.1)

# Convert to log+1 scale for better visual separation without negative axes
degree_df["in_degree_log"] = degree_df["in_degree"] + 1
degree_df["out_degree_log"] = degree_df["out_degree"] + 1

import matplotlib.colors as mcolors

fig, ax = plt.subplots(figsize=(14, 11))

sc = ax.scatter(
    degree_df["out_degree_log"],
    degree_df["in_degree_log"],
    s=30,
    alpha=0.15,
    c=degree_df["authority_score"],
    cmap="viridis",
    edgecolors="none",
    norm=mcolors.LogNorm(vmin=0.1, vmax=degree_df["authority_score"].max()),
    label="Other Artists"
)

top_df = degree_df[degree_df["artist"].isin(top_15_vol)]
ax.scatter(
    top_df["out_degree_log"],
    top_df["in_degree_log"],
    s=250,
    c="#E74C3C",
    edgecolors="#C0392B",
    linewidth=2.5,
    zorder=5,
    label="Top 15 Most Sampled"
)

# Only label the top 7 to avoid extreme clutter while retaining the 15 red dots
labels_to_show = top_15_vol[:7]

top_7_sorted = top_df[top_df["artist"].isin(labels_to_show)].sort_values(by="in_degree_log", ascending=False)
y_vals = np.logspace(np.log10(120), np.log10(320), num=len(top_7_sorted))[::-1]

for idx, (_, row) in enumerate(top_7_sorted.iterrows()):
    label = row["artist"] if len(row["artist"]) <= 18 else row["artist"][:16] + "..."
    ax.annotate(
        label,
        xy=(row["out_degree_log"], row["in_degree_log"]),
        xytext=(350, y_vals[idx]),
        textcoords="data",
        fontsize=10,
        fontweight="bold",
        ha="left",
        va="center",
        bbox=dict(
            boxstyle="round,pad=0.3",
            facecolor="#FFFACD",
            edgecolor="#E74C3C",
            alpha=0.9,
        ),
        arrowprops=dict(arrowstyle="-|>", color="#E74C3C", lw=1.0, alpha=0.7, shrinkB=0)
    )

median_in = degree_df["in_degree_log"].median()
median_out = degree_df["out_degree_log"].median()
ax.axhline(y=median_in, color="gray", linestyle="--", alpha=0.5, linewidth=1.5)
ax.axvline(x=median_out, color="gray", linestyle="--", alpha=0.5, linewidth=1.5)

# Anchor quadrant labels to axes relative coordinates so they never overlap
ax.text(
    0.99,
    0.98,
    "BRIDGES\n(Sample & Get Sampled)",
    transform=ax.transAxes,
    ha="right",
    va="top",
    fontsize=11,
    fontweight="bold",
    bbox=dict(boxstyle="round", facecolor="#E8F5E9", alpha=0.8),
)
ax.text(
    0.05,
    0.95,
    "AUTHORITIES\n(Pure Sources)",
    transform=ax.transAxes,
    ha="left",
    va="top",
    fontsize=11,
    fontweight="bold",
    bbox=dict(boxstyle="round", facecolor="#FFF3E0", alpha=0.8),
)
ax.text(
    0.95,
    0.05,
    "HEAVY SAMPLERS\n(Use Many Samples)",
    transform=ax.transAxes,
    ha="right",
    va="bottom",
    fontsize=11,
    fontweight="bold",
    bbox=dict(boxstyle="round", facecolor="#E3F2FD", alpha=0.8),
)
ax.text(
    0.05,
    0.05,
    "PERIPHERAL\n(Low Activity)",
    transform=ax.transAxes,
    ha="left",
    va="bottom",
    fontsize=11,
    fontweight="bold",
    bbox=dict(boxstyle="round", facecolor="#F5F5F5", alpha=0.8),
)

ax.set_xlabel("Out-Degree (Number of Samples Used + 1)", fontsize=13, fontweight="bold")
ax.set_ylabel("In-Degree (Times Being Sampled + 1)", fontsize=13, fontweight="bold")
ax.set_title(
    "Artist Hub Analysis: Sampling Behavior\n(Color = PageRank Authority, Scale = Log-Log)",
    fontsize=16,
    fontweight="bold",
    pad=20,
)
ax.set_xscale("log")
ax.set_yscale("log")

cbar = plt.colorbar(sc, ax=ax)
cbar.set_label("Authority Score (PageRank)", fontsize=11, fontweight="bold")

ax.grid(True, alpha=0.3, linestyle=":", linewidth=1)
ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.15), ncol=2, fontsize=11, framealpha=0.95)

plt.tight_layout()
plt.savefig(f"{output_dir}/fig7_hub_analysis.png", dpi=300, bbox_inches="tight")
plt.savefig(f"{output_dir}/fig7_hub_analysis.pdf", bbox_inches="tight")
print(f"✓ Saved: fig7_hub_analysis.png/pdf\n")
plt.close()

# Copy generated figures to report/Immagini/ for LaTeX compilation
import shutil
report_img_dir = "../report/Immagini"
if os.path.exists(report_img_dir):
    for fname in os.listdir("figures/report_figures"):
        src = os.path.join("figures/report_figures", fname)
        dst = os.path.join(report_img_dir, fname)
        if os.path.isfile(src):
            shutil.copy2(src, dst)
            print(f"   ✓ Copied {fname} to {report_img_dir}/")

spark.stop()
print("=" * 80)
print("✓ ALL VISUALIZATIONS COMPLETE!")
print("=" * 80)
