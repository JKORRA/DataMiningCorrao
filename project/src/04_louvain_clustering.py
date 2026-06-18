"""
Louvain Community Detection

This script performs network community detection using the Louvain method via NetworkX.
It identifies dense clusters of artists (musical communities or genres) based on their
sampling behavior.

The pipeline performs the following steps:
1. Loads the edge list from the pre-processed Parquet file.
2. Builds a directed weighted artist graph.
3. Converts the graph to an undirected weighted graph (required by the Louvain algorithm).
4. Runs the Louvain heuristic to partition the network into distinct communities.
5. Identifies a "representative" artist for each community (the artist with the highest degree).
6. Saves the cluster assignments as Parquet and CSV files for downstream analysis.
"""

import pandas as pd
import networkx as nx
from networkx.algorithms.community import louvain_communities
import pyarrow as pa
import pyarrow.parquet as pq
import os

print("Loading graph...")
df = pd.read_parquet("outputs/music_graph.parquet")

# Remove self-loops using Artist Names
# Self-loops represent an artist sampling themselves, which inflates their degree
# but provides no structural information about community membership.
self_loops = (df["Sampler_Artist_Name"] == df["Original_Artist_Name"]).sum()
if self_loops > 0:
    print(f"Removing {self_loops} self-loops...")
    df = df[df["Sampler_Artist_Name"] != df["Original_Artist_Name"]]

print(f"Graph loaded: {len(df)} edges")

# Build directed graph from artist-level edges
# Edge direction: Sampler -> Original (authority flows from sampler to the original artist)
print("Building artist graph...")
G = nx.DiGraph()
for _, row in df.iterrows():
    u = row["Sampler_Artist_Name"]
    v = row["Original_Artist_Name"]
    w = row.get("weight", 1.0)
    if G.has_edge(u, v):
        G[u][v]["weight"] += w  # Aggregate weights for multiple samples between the same artists
    else:
        G.add_edge(u, v, weight=w)

print(f"Artist graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} directed edges")

# Convert to undirected for Louvain (aggregate parallel edge weights)
# Louvain requires an undirected graph. We sum the weights of reciprocal edges (A->B and B->A).
print("Converting to undirected graph for community detection...")
G_undirected = nx.Graph()
for u, v, d in G.edges(data=True):
    w = d.get("weight", 1.0)
    if G_undirected.has_edge(u, v):
        G_undirected[u][v]["weight"] += w
    else:
        G_undirected.add_edge(u, v, weight=w)

print(f"Undirected graph: {G_undirected.number_of_nodes()} nodes, {G_undirected.number_of_edges()} edges")

# Louvain community detection
# The resolution parameter controls the size of the communities.
# resolution=1.0 is the standard Newman-Girvan modularity.
print("Running Louvain community detection (resolution=1.0)...")
communities = list(louvain_communities(G_undirected, weight="weight", resolution=1.0, seed=42))
print(f"Found {len(communities)} communities")

# Assign community IDs (0-indexed) and pick a representative per cluster
# The representative is chosen as the node with the highest weighted degree within that community.
weighted_deg = dict(G_undirected.degree(weight="weight"))

community_map = {}
rep_map = {}
for i, comm in enumerate(communities):
    # Pick the node with highest weighted degree as cluster representative
    rep = max(comm, key=lambda n: weighted_deg.get(n, 0))
    rep_map[i] = rep
    for node in comm:
        community_map[node] = i

# Build results DataFrame
artist_names = list(community_map.keys())
cluster_ids = [community_map[a] for a in artist_names]
cluster_reps = [rep_map[community_map[a]] for a in artist_names]

df_labels = pd.DataFrame({
    "artist_name": artist_names,
    "cluster_id": cluster_ids,
    "cluster_representative": cluster_reps
})

# Cluster sizes
cluster_sizes = df_labels.groupby("cluster_representative").size().reset_index(name="cluster_size")
cluster_sizes = cluster_sizes.sort_values("cluster_size", ascending=False)

print("\n--- TOP 20 LARGEST MUSICAL COMMUNITIES ---")
print("These artists define the largest 'genealogical families' in music:")
print(cluster_sizes.head(20).to_string(index=False))

# Save results
print("Saving results...")
os.makedirs("outputs", exist_ok=True)

# Labels as Parquet (compatible with Spark-based downstream scripts)
table = pa.Table.from_pandas(df_labels)
pq.write_table(table, "outputs/music_labels.parquet")

# Cluster sizes as CSV
cluster_sizes.head(100).to_csv("outputs/music_clusters.csv", index=False)

# Also save cluster membership (artist → cluster_id) as CSV for easy inspection
df_labels.to_csv("outputs/music_cluster_membership.csv", index=False)

print("✓ Clusters saved to: music_labels.parquet, music_clusters.csv, music_cluster_membership.csv")
