"""
Cluster Visualization
Visualizes the music genealogy clusters using network graphs.
"""

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as _max, first, greatest, count

spark = SparkSession.builder \
    .appName("MusicGenealogy_FinalViz") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Loading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")

# Quick cluster computation
print("Recomputing clusters...")
edges = df_graph.select(col("source_song_id").alias("child"), col("target_song_id").alias("parent")).distinct()
nodes = edges.select("child").union(edges.select("parent")).distinct()
labels = nodes.select(col("child").alias("id"), col("child").alias("label"))

# Run 3 iterations of label propagation
for i in range(3):
    propagation = edges.join(labels, edges.parent == labels.id).select(col("child"), col("label").alias("parent_label"))
    current_state = labels.alias("l").join(propagation.alias("p"), col("l.id") == col("p.child"), "left_outer") \
        .select(col("l.id"), col("l.label").alias("old_label"), col("p.parent_label").alias("new_proposal"))
    labels = current_state.groupBy("id").agg(_max(col("new_proposal")).alias("max_proposal"), first("old_label").alias("old")) \
        .select(col("id"), greatest(col("max_proposal"), col("old")).alias("label"))

# Map artists to clusters
song_artist_map = df_graph.select(col("target_song_id").alias("song_id"), col("Original_Artist_Name").alias("artist_name")).distinct()

artist_clusters = labels.join(song_artist_map, labels.label == song_artist_map.song_id) \
    .select(col("artist_name"), col("label").alias("cluster_id")) \
    .dropDuplicates(["artist_name"])

# Select top connections for visualization
print("Selecting top connections...")

try:
    pagerank_df = spark.read.parquet("artist_pagerank.parquet")
    top_artists_list = [row['artist'] for row in pagerank_df.orderBy(col("authority_score").desc()).limit(30).collect()]
    print(f"Selected top 30 artists by PageRank")
except:
    print("PageRank not found, using alternative method")
    top_artists_list = None

if top_artists_list:
    top_edges = df_graph.filter(
        (col("Original_Artist_Name").isin(top_artists_list)) | 
        (col("Sampler_Artist_Name").isin(top_artists_list))
    ).filter(col("Original_Artist_Name") != "Ninja McTits") \
     .filter(col("Sampler_Artist_Name") != "Ninja McTits") \
     .groupBy("Sampler_Artist_Name", "Original_Artist_Name") \
     .agg(count("*").alias("weight")) \
     .filter(col("weight") > 1) \
     .orderBy(col("weight").desc()) \
     .limit(80)
else:
    top_edges = df_graph.filter(col("Original_Artist_Name") != "Ninja McTits") \
        .filter(col("Sampler_Artist_Name") != "Ninja McTits") \
        .groupBy("Sampler_Artist_Name", "Original_Artist_Name") \
        .agg(count("*").alias("weight")) \
        .orderBy(col("weight").desc()) \
        .limit(60)

viz_data = top_edges.join(artist_clusters, top_edges.Original_Artist_Name == artist_clusters.artist_name, "left") \
    .select(
        col("Sampler_Artist_Name"), 
        col("Original_Artist_Name"), 
        col("weight"),
        col("cluster_id")
    )

pdf = viz_data.toPandas()
spark.stop()

# Generate graph visualization
print(f"Generating graph with {len(pdf)} edges...")

G = nx.DiGraph()

for _, row in pdf.iterrows():
    sampler = row['Sampler_Artist_Name']
    original = row['Original_Artist_Name']
    weight = row['weight']
    clus_id = row['cluster_id']
    
    G.add_node(original, cluster=clus_id)
    if sampler not in G.nodes:
        G.add_node(sampler, cluster=clus_id)
        
    G.add_edge(sampler, original, weight=weight)

# Assign colors based on clusters
unique_clusters = sorted(list(set(pdf['cluster_id'])))
colors = cm.rainbow(np.linspace(0, 1, len(unique_clusters)))
cluster_color_map = {cid: colors[i] for i, cid in enumerate(unique_clusters)}

node_colors = []
for node in G.nodes():
    cid = G.nodes[node].get('cluster', -1)
    node_colors.append(cluster_color_map.get(cid, (0.8, 0.8, 0.8, 1)))

# Set node sizes based on degree
d = dict(G.degree)
node_sizes = [v * 100 + 300 for v in d.values()]

# Use spring layout
pos = nx.spring_layout(G, k=0.9, iterations=100, seed=42)

plt.figure(figsize=(18, 12), facecolor='#f0f0f0')

nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors, alpha=0.8, edgecolors='white')
nx.draw_networkx_edges(G, pos, edge_color='gray', alpha=0.4, arrowstyle='->', arrowsize=20)

labels = {n: n for n in G.nodes() if G.degree(n) > 0} 
nx.draw_networkx_labels(G, pos, labels=labels, font_size=9, font_weight='bold')

plt.title("Music Genealogy: Top Families", fontsize=20)
plt.axis('off')

filename = "music_genealogy_final.png"
plt.savefig(filename, dpi=150, bbox_inches='tight')
print(f"Graph saved: {filename}")
plt.show()
