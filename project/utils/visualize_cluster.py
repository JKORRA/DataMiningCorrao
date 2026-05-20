"""
Cluster-Colored Artist Network
Light-theme network graph showing top artists by PageRank, colored by their Louvain cluster.
Intra-cluster edges in muted cluster color, inter-cluster edges in gray.
"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import os
import shutil

plt.rcParams["font.sans-serif"] = ["Noto Sans CJK JP", "Arial Unicode MS", "DejaVu Sans", "Droid Sans Fallback", "IPAGothic", "IPAMincho", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 11
plt.rcParams["axes.labelsize"] = 12
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["legend.fontsize"] = 9

output_dir = "figures/report_figures"
os.makedirs(output_dir, exist_ok=True)

TOP_N = 50
MIN_EDGE_WEIGHT = 1
MAX_LEGEND_CLUSTERS = 10

spark = (
    SparkSession.builder.appName("ClusterArtistNetwork")
    .config("spark.driver.memory", "6g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("Loading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")
df_labels = spark.read.parquet("outputs/music_labels.parquet")

print("Selecting top artists by PageRank...")
top_artists_rows = df_pagerank.orderBy(desc("authority_score")).limit(TOP_N).collect()
top_artists = [row['artist'] for row in top_artists_rows]
pagerank_scores = {row['artist']: row['authority_score'] for row in top_artists_rows}

print("Getting artist-to-cluster mapping...")
artist_cluster_pd = df_labels.select("artist_name", "cluster_representative").dropDuplicates(["artist_name"]).toPandas()
cluster_map = dict(zip(artist_cluster_pd["artist_name"], artist_cluster_pd["cluster_representative"]))

print(f"Getting edges among top {TOP_N} artists...")
edges_pd = (
    df_graph.filter(
        col("Sampler_Artist_Name").isin(top_artists) &
        col("Original_Artist_Name").isin(top_artists)
    )
    .groupBy("Sampler_Artist_Name", "Original_Artist_Name")
    .agg(count("*").alias("weight"))
    .filter(col("weight") >= MIN_EDGE_WEIGHT)
    .toPandas()
)

print(f"Edges found: {len(edges_pd)}")

spark.stop()

G = nx.DiGraph()

for a in top_artists:
    G.add_node(a)

for _, row in edges_pd.iterrows():
    G.add_edge(row["Sampler_Artist_Name"], row["Original_Artist_Name"], weight=row["weight"])

print(f"Graph has {len(G.nodes)} nodes, {len(G.edges)} edges")

degree_dict = dict(G.degree())

cluster_colors = {}
for node in G.nodes():
    clus = cluster_map.get(node, "Unknown")
    if clus not in cluster_colors:
        cluster_colors[clus] = None

unique_clusters = sorted(cluster_colors.keys(), key=lambda c: sum(1 for n in G.nodes() if cluster_map.get(n, "Unknown") == c), reverse=True)

cmap_name = "tab20" if len(unique_clusters) > 10 else "tab10"
cmap = plt.colormaps[cmap_name]
cluster_color_map = {clus: cmap(i % cmap.N) for i, clus in enumerate(unique_clusters)}
cluster_color_map["Unknown"] = (0.6, 0.6, 0.6, 1)

node_colors = []
for node in G.nodes():
    clus = cluster_map.get(node, "Unknown")
    node_colors.append(cluster_color_map.get(clus, (0.6, 0.6, 0.6, 1)))

pr_values = list(pagerank_scores.values())
min_pr, max_pr = min(pr_values), max(pr_values)
if max_pr > min_pr:
    node_sizes = [500 + 2000 * (np.sqrt(max(0, pagerank_scores.get(node, min_pr) - min_pr)) / np.sqrt(max(1e-9, max_pr - min_pr))) for node in G.nodes()]
else:
    node_sizes = [800 for node in G.nodes()]

pos = nx.spring_layout(G, k=2.5, iterations=200, seed=42)

fig, ax = plt.subplots(figsize=(20, 16))

edges_drawn = G.edges()
edge_weights = [G[u][v].get("weight", 1) for u, v in edges_drawn]
max_w = max(edge_weights) if edge_weights else 1

for u, v in edges_drawn:
    w = G[u][v].get("weight", 1)
    width = 0.5 + 2.5 * (w / max_w)

    u_cluster = cluster_map.get(u, "Unknown")
    v_cluster = cluster_map.get(v, "Unknown")

    if u_cluster == v_cluster and u_cluster != "Unknown":
        base_color = cluster_color_map.get(u_cluster, (0.5, 0.5, 0.5))
        edge_color = (base_color[0], base_color[1], base_color[2], 0.35)
        style = "solid"
    else:
        edge_color = (0.6, 0.6, 0.6, 0.15)
        style = "dashed"

    ax.annotate(
        "",
        xy=pos[v], xytext=pos[u],
        arrowprops=dict(
            arrowstyle="-|>", color=edge_color, lw=width,
            connectionstyle="arc3,rad=0.12", linestyle=style,
            alpha=0.7,
        ),
    )

nx.draw_networkx_nodes(
    G, pos, ax=ax,
    node_size=node_sizes,
    node_color=node_colors,
    edgecolors="white",
    linewidths=1.5,
    alpha=0.9,
)

for node in G.nodes():
    if node in pos:
        x, y = pos[node]
        pr_score = pagerank_scores.get(node, min_pr)
        if max_pr > min_pr:
            pr_norm = np.sqrt(max(0, pr_score - min_pr)) / np.sqrt(max(1e-9, max_pr - min_pr))
        else:
            pr_norm = 0.5
        fs = min(13, max(8, 8 + pr_norm * 5))
        ax.text(
            x, y, node,
            fontsize=fs, fontweight="bold",
            ha="center", va="center",
            bbox=dict(boxstyle="round,pad=0.1", facecolor="white", edgecolor="none", alpha=0.85),
        )

legend_patches = []
shown = 0
for clus in unique_clusters:
    if clus == "Unknown":
        continue
    if shown >= MAX_LEGEND_CLUSTERS:
        break
    count_in_graph = sum(1 for n in G.nodes() if cluster_map.get(n, "Unknown") == clus)
    color = cluster_color_map[clus]
    label = f"{clus} ({count_in_graph})"
    legend_patches.append(mpatches.Patch(color=color, label=label))
    shown += 1

if "Unknown" in cluster_color_map:
    unknown_count = sum(1 for n in G.nodes() if cluster_map.get(n, "Unknown") == "Unknown")
    if unknown_count > 0:
        legend_patches.append(mpatches.Patch(color=(0.6, 0.6, 0.6), label=f"Unknown ({unknown_count})"))

ax.legend(
    handles=legend_patches,
    loc="upper left",
    framealpha=0.9,
    fontsize=8,
    title="Cluster (artists in graph)",
    title_fontsize=9,
)

ax.set_title(
    f"Top {TOP_N} Influential Artists by Cluster\n(Node size based on PageRank authority, Color = Louvain cluster, Solid edges = intra-cluster, Dashed = inter-cluster)",
    fontsize=15, fontweight="bold", pad=20,
)
ax.text(
    0.5, 0.98,
    "Arrow: sampler → original artist",
    transform=ax.transAxes, ha="center", va="top",
    fontsize=9, color="gray", style="italic",
)
ax.axis("off")

plt.tight_layout()

png_path = f"{output_dir}/fig4_cluster_artist_network.png"
pdf_path = f"{output_dir}/fig4_cluster_artist_network.pdf"
plt.savefig(png_path, dpi=300, bbox_inches="tight")
plt.savefig(pdf_path, bbox_inches="tight")
print(f"Saved: {png_path}")
print(f"Saved: {pdf_path}")
plt.close()

report_img_dir = "../report/Immagini"
if os.path.exists(report_img_dir):
    shutil.copy2(png_path, os.path.join(report_img_dir, "fig4_cluster_artist_network.png"))
    shutil.copy2(pdf_path, os.path.join(report_img_dir, "fig4_cluster_artist_network.pdf"))
    print(f"Copied to {report_img_dir}/")

print("Done!")
