"""
Music Genealogy Network Visualizations
Creates publication-quality network visualizations for the music sampling network.
Filters out [unknown] artists and unreadable characters for clean presentation.
"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "sans-serif"]
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
node_sizes = [volume_dict.get(node, 5) * 5 for node in G_top.nodes()]
node_colors = [
    "#FF6B6B" if node in top_artists else "#A0D8EF" for node in G_top.nodes()
]

pos = nx.spring_layout(G_top, k=2.5, iterations=200, seed=42)
fig, ax = plt.subplots(figsize=(18, 14))

edges = G_top.edges()
if edges:
    weights = [G_top[u][v]["weight"] for u, v in edges]
    nx.draw_networkx_edges(
        G_top,
        pos,
        edge_color="#666666",
        width=[w * 0.8 / 3 for w in weights],
        alpha=0.4,
        arrows=True,
        arrowsize=15,
        arrowstyle="->",
        connectionstyle="arc3,rad=0.1",
        node_size=node_sizes,
        ax=ax,
    )

nx.draw_networkx_nodes(
    G_top,
    pos,
    node_size=node_sizes,
    node_color=node_colors,
    edgecolors="#2C5AA0",
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
            fontsize=10,
            fontweight="bold",
            ha="center",
            va="center",
            bbox=dict(
                boxstyle="round,pad=0.4",
                facecolor="white",
                edgecolor="#2C5AA0",
                linewidth=1.5,
                alpha=0.95,
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
# FIGURE 5: SAMPLING FLOW - BIPARTITE
# =============================================================================
print("=" * 80)
print("[FIGURE 5] Sampling Flow - Samplers → Sampled")
print("=" * 80)

# Top 10 samplers by out-degree, readable names only
all_samplers = (
    df_graph.filter(col("Sampler_Artist_Name") != "[unknown]")
    .groupBy("Sampler_Artist_Name")
    .agg(count("*").alias("total_samples"))
    .orderBy(desc("total_samples"))
    .toPandas()
)
all_samplers = all_samplers[all_samplers["Sampler_Artist_Name"].apply(is_readable)]
top_samplers = all_samplers.head(10)["Sampler_Artist_Name"].tolist()

# Top 10 sampled by in-degree, readable names only
all_sampled = (
    df_graph.filter(col("Original_Artist_Name") != "[unknown]")
    .groupBy("Original_Artist_Name")
    .agg(count("*").alias("times_sampled"))
    .orderBy(desc("times_sampled"))
    .toPandas()
)
all_sampled = all_sampled[all_sampled["Original_Artist_Name"].apply(is_readable)]
top_sampled = all_sampled.head(10)["Original_Artist_Name"].tolist()

flow_edges = (
    df_graph.filter(
        col("Sampler_Artist_Name").isin(top_samplers)
        & col("Original_Artist_Name").isin(top_sampled)
    )
    .groupBy("Sampler_Artist_Name", "Original_Artist_Name")
    .agg(count("*").alias("weight"))
    .filter(col("weight") >= 2)
    .toPandas()
)

print(f"Found {len(flow_edges)} connections (weight >= 2)")

G_flow = nx.DiGraph()
for sampler in top_samplers:
    G_flow.add_node(sampler, bipartite=0)
for sampled in top_sampled:
    G_flow.add_node(sampled, bipartite=1)

for _, row in flow_edges.iterrows():
    G_flow.add_edge(
        row["Sampler_Artist_Name"], row["Original_Artist_Name"], weight=row["weight"]
    )

left_nodes = [n for n in G_flow.nodes() if G_flow.nodes[n]["bipartite"] == 0]
right_nodes = [n for n in G_flow.nodes() if G_flow.nodes[n]["bipartite"] == 1]

pos = {}
y_spacing = 1.0
for i, node in enumerate(left_nodes):
    pos[node] = (0, i * y_spacing)
for i, node in enumerate(right_nodes):
    pos[node] = (3, i * y_spacing)

fig, ax = plt.subplots(figsize=(16, 12))

for u, v, data in G_flow.edges(data=True):
    weight = data["weight"]
    ax.plot(
        [pos[u][0], pos[v][0]],
        [pos[u][1], pos[v][1]],
        "#888888",
        linewidth=min(weight * 0.6, 6),
        alpha=0.5,
        zorder=1,
    )

for node in left_nodes:
    ax.scatter(
        pos[node][0],
        pos[node][1],
        s=800,
        c="#87CEEB",
        edgecolors="#4682B4",
        linewidth=3,
        zorder=2,
    )
    ax.text(
        pos[node][0] - 0.3,
        pos[node][1],
        node,
        ha="right",
        va="center",
        fontsize=11,
        fontweight="bold",
    )

for node in right_nodes:
    ax.scatter(
        pos[node][0],
        pos[node][1],
        s=800,
        c="#FFB6C1",
        edgecolors="#DC143C",
        linewidth=3,
        zorder=2,
    )
    ax.text(
        pos[node][0] + 0.3,
        pos[node][1],
        node,
        ha="left",
        va="center",
        fontsize=11,
        fontweight="bold",
    )

ax.text(
    0,
    -1,
    "TOP SAMPLERS\n(High Out-Degree)",
    ha="center",
    fontsize=13,
    fontweight="bold",
    color="#4682B4",
)
ax.text(
    3,
    -1,
    "TOP SAMPLED\n(High In-Degree / Authority)",
    ha="center",
    fontsize=13,
    fontweight="bold",
    color="#DC143C",
)

ax.set_title(
    "Sampling Flow: Who Samples Whom?\n(Line Thickness = Number of Samples, Only connections ≥ 2 shown)",
    fontsize=16,
    fontweight="bold",
    pad=25,
)
ax.set_xlim(-1, 4)
ax.set_ylim(-2, max(len(left_nodes), len(right_nodes)))
ax.axis("off")
plt.tight_layout()
plt.savefig(f"{output_dir}/fig6_sampling_flow.png", dpi=300, bbox_inches="tight")
plt.savefig(f"{output_dir}/fig6_sampling_flow.pdf", bbox_inches="tight")
print(f"✓ Saved: fig6_sampling_flow.png/pdf\n")
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

fig, ax = plt.subplots(figsize=(14, 11))

ax.scatter(
    degree_df["out_degree"],
    degree_df["in_degree"],
    s=40,
    alpha=0.4,
    c="#B0B0B0",
    edgecolors="none",
    label="Other Artists",
)

top_df = degree_df[degree_df["artist"].isin(top_15_vol)]
ax.scatter(
    top_df["out_degree"],
    top_df["in_degree"],
    s=250,
    c="#E74C3C",
    edgecolors="#C0392B",
    linewidth=2.5,
    zorder=5,
    label="Top 15 Most Sampled",
)

for _, row in top_df.iterrows():
    label = row["artist"] if len(row["artist"]) <= 18 else row["artist"][:16] + "..."
    ax.annotate(
        label,
        (row["out_degree"], row["in_degree"]),
        xytext=(10, 8),
        textcoords="offset points",
        fontsize=9,
        fontweight="bold",
        bbox=dict(
            boxstyle="round,pad=0.4",
            facecolor="#FFFACD",
            edgecolor="#E74C3C",
            alpha=0.9,
        ),
        arrowprops=dict(
            arrowstyle="->", connectionstyle="arc3,rad=0.2", color="#E74C3C", lw=1.5
        ),
    )

median_in = degree_df["in_degree"].median()
median_out = degree_df["out_degree"].median()
ax.axhline(y=median_in, color="gray", linestyle="--", alpha=0.5, linewidth=1.5)
ax.axvline(x=median_out, color="gray", linestyle="--", alpha=0.5, linewidth=1.5)

max_out = degree_df["out_degree"].max()
max_in = degree_df["in_degree"].max()

ax.text(
    max_out * 0.75,
    max_in * 0.85,
    "BRIDGES\n(Sample & Get Sampled)",
    ha="center",
    fontsize=11,
    fontweight="bold",
    bbox=dict(boxstyle="round", facecolor="#E8F5E9", alpha=0.8),
)
ax.text(
    max_out * 0.15,
    max_in * 0.85,
    "AUTHORITIES\n(Pure Sources)",
    ha="center",
    fontsize=11,
    fontweight="bold",
    bbox=dict(boxstyle="round", facecolor="#FFF3E0", alpha=0.8),
)
ax.text(
    max_out * 0.75,
    max_in * 0.15,
    "HEAVY SAMPLERS\n(Use Many Samples)",
    ha="center",
    fontsize=11,
    fontweight="bold",
    bbox=dict(boxstyle="round", facecolor="#E3F2FD", alpha=0.8),
)

ax.set_xlabel("Out-Degree (Number of Samples Used)", fontsize=13, fontweight="bold")
ax.set_ylabel("In-Degree (Times Being Sampled)", fontsize=13, fontweight="bold")
ax.set_title(
    "Artist Hub Analysis: Sampling Behavior\n(Red = Top 15 Most Sampled Artists)",
    fontsize=16,
    fontweight="bold",
    pad=20,
)
ax.grid(True, alpha=0.3, linestyle=":", linewidth=1)
ax.legend(loc="upper right", fontsize=11, framealpha=0.95)

plt.tight_layout()
plt.savefig(f"{output_dir}/fig7_hub_analysis.png", dpi=300, bbox_inches="tight")
plt.savefig(f"{output_dir}/fig7_hub_analysis.pdf", bbox_inches="tight")
print(f"✓ Saved: fig7_hub_analysis.png/pdf\n")
plt.close()

spark.stop()
print("=" * 80)
print("✓ ALL VISUALIZATIONS COMPLETE!")
print("=" * 80)
