"""
Cluster Visualization
Visualizes the music genealogy clusters using network graphs.
Uses a modern dark theme with readable labels.
"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "IPAGothic", "IPAMincho", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

spark = SparkSession.builder \
    .appName("MusicGenealogy_FinalViz") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Loading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_labels = spark.read.parquet("outputs/music_labels.parquet")

# Select top connections for visualization
print("Selecting top connections...")

try:
    pagerank_df = spark.read.parquet("outputs/artist_pagerank.parquet")
    top_artists_list = [row['artist'] for row in pagerank_df.orderBy(col("authority_score").desc()).limit(40).collect()]
    print(f"Selected top 40 artists by PageRank")
except Exception as e:
    print(f"PageRank not found ({e}), using degree-based fallback")
    # Fallback: use artists that appear most frequently in edges
    top_sampler = df_graph.groupBy("Sampler_Artist_Name").agg(count("*").alias("cnt"))
    top_original = df_graph.groupBy("Original_Artist_Name").agg(count("*").alias("cnt"))
    top_artists_list = [r[0] for r in top_sampler.union(top_original).orderBy(col("cnt").desc()).limit(40).collect()]

# Get edges involving top artists (on BOTH sides for cleaner subgraph)
top_edges = df_graph.filter(
    col("Original_Artist_Name").isin(top_artists_list) &
    col("Sampler_Artist_Name").isin(top_artists_list)
).groupBy("Sampler_Artist_Name", "Original_Artist_Name") \
 .agg(count("*").alias("weight")) \
 .orderBy(col("weight").desc()) \
 .limit(120)

# Get cluster info for coloring
viz_data = top_edges.join(
    df_labels.select(col("artist_name"), col("cluster_id"), col("cluster_representative")),
    top_edges.Original_Artist_Name == df_labels.artist_name,
    "left"
).select(
    col("Sampler_Artist_Name"),
    col("Original_Artist_Name"),
    col("weight"),
    col("cluster_id")
)

pdf = viz_data.toPandas()

# Also try to get cluster info for ALL nodes via artist names
all_artists_in_graph = set(pdf["Sampler_Artist_Name"].tolist() + pdf["Original_Artist_Name"].tolist())

spark.stop()

# Generate graph visualization
print(f"Generating graph with {len(pdf)} edges...")

G = nx.DiGraph()

for _, row in pdf.iterrows():
    sampler = row['Sampler_Artist_Name']
    original = row['Original_Artist_Name']
    weight = row['weight']
    clus_id = row['cluster_id'] if pd.notna(row['cluster_id']) else "unknown"

    G.add_node(original, cluster=clus_id)
    if sampler not in G.nodes:
        G.add_node(sampler, cluster=clus_id)

    G.add_edge(sampler, original, weight=weight)

if len(G.nodes) == 0:
    print("WARNING: No edges found for top artists. The graph is empty.")
    exit(0)

# Assign colors based on clusters
unique_clusters = sorted(list(set(nx.get_node_attributes(G, 'cluster').values())))
# Use a perceptually distinct colormap
cmap = plt.colormaps["Set2"].resampled(max(len(unique_clusters), 8))
cluster_color_map = {cid: cmap(i) for i, cid in enumerate(unique_clusters)}

node_colors = []
for node in G.nodes():
    cid = G.nodes[node].get('cluster', 'unknown')
    node_colors.append(cluster_color_map.get(cid, (0.6, 0.6, 0.6, 1)))

# Set node sizes based on degree
d = dict(G.degree)
node_sizes = [v * 120 + 400 for v in d.values()]

# Use spring layout with higher repulsion for readability
pos = nx.spring_layout(G, k=2.2, iterations=150, seed=42)

# --- Modern dark theme ---
fig, ax = plt.subplots(figsize=(22, 15), facecolor='#0f0e17')
ax.set_facecolor('#0f0e17')

# Draw edges with glow effect
edge_weights = [G[u][v].get('weight', 1) for u, v in G.edges()]
max_w = max(edge_weights) if edge_weights else 1

# Subtle glow layer
nx.draw_networkx_edges(
    G, pos, ax=ax,
    edge_color='#ff890640',
    width=[0.5 + 2.5 * (w / max_w) for w in edge_weights],
    alpha=0.3,
    arrowstyle='->', arrowsize=18,
    connectionstyle='arc3,rad=0.08'
)
# Main edge layer
nx.draw_networkx_edges(
    G, pos, ax=ax,
    edge_color='#ffffff18',
    width=[0.3 + 1.5 * (w / max_w) for w in edge_weights],
    alpha=0.6,
    arrowstyle='->', arrowsize=14,
    connectionstyle='arc3,rad=0.08'
)

# Draw nodes with border
nx.draw_networkx_nodes(
    G, pos, ax=ax,
    node_size=node_sizes,
    node_color=node_colors,
    alpha=0.92,
    edgecolors='white',
    linewidths=1.2
)

# Draw labels with background boxes for readability
for node, (x, y) in pos.items():
    deg = G.degree(node)
    fontsize = min(10, max(7, 6 + deg * 0.5))
    ax.text(
        x, y + 0.03,
        node,
        fontsize=fontsize,
        fontweight='bold',
        color='#fffffe',
        ha='center', va='center',
        bbox=dict(
            boxstyle='round,pad=0.25',
            facecolor='#0f0e17',
            edgecolor='none',
            alpha=0.75
        )
    )

ax.set_title(
    "Music Genealogy: Top Influential Families",
    fontsize=22, fontweight='bold', color='#ff8906',
    pad=20
)
ax.text(
    0.5, 0.97,
    "Arrow: sampler → original artist  |  Node size = connection degree  |  Color = cluster",
    transform=ax.transAxes, ha='center', va='top',
    fontsize=10, color=(1, 1, 1, 0.4),
    style='italic'
)
ax.axis('off')

filename = "outputs/music_genealogy_final.png"
plt.savefig(filename, dpi=180, bbox_inches='tight', facecolor='#0f0e17')
print(f"✓ Graph saved: {filename}")
plt.close()
