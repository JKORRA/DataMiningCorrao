"""
Advanced Data Mining Experiments for Music Genealogy
Implements Bridge Discovery, Insular Community Hypothesis, and Macro-Flow Analysis.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sqrt as _sqrt, lit, when, row_number, sum as _sum
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import networkx as nx
import os
import warnings

warnings.filterwarnings("ignore")

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 11

# Add CJK fallback font
plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "IPAGothic", "IPAMincho", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

output_dir = "figures/report_figures"
os.makedirs(output_dir, exist_ok=True)

def is_readable(name):
    if not isinstance(name, str) or name in ("[unknown]", "[no artist]", ""):
        return False
    return name.isprintable()

print("=" * 80)
print("ADVANCED DATA MINING EXPERIMENTS")
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
    print("Warning: music_labels.parquet not found. Make sure to run clustering first.")
    df_labels = None

# =============================================================================
# EXPERIMENT 1: BRIDGE DISCOVERY
# =============================================================================
print("\n[Experiment 1] Discovering Evolutionary Bridges...")

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

bridges = in_degree.join(out_degree, "artist", "inner")
bridges = bridges.withColumn("bridge_score", _sqrt(col("in_degree") * col("out_degree")))

bridges_pd = bridges.orderBy(desc("bridge_score")).toPandas()
bridges_pd = bridges_pd[bridges_pd["artist"].apply(is_readable)].head(15)

fig, ax = plt.subplots(figsize=(12, 8))
y_pos = np.arange(len(bridges_pd))
ax.barh(y_pos, bridges_pd["bridge_score"], color="#9b59b6", edgecolor="#8e44ad", height=0.7)
ax.set_yticks(y_pos)
ax.set_yticklabels(bridges_pd["artist"], fontweight="bold")
ax.invert_yaxis()

for i, v in enumerate(bridges_pd["bridge_score"]):
    ax.text(v + 1, i, f"{v:.1f}", va='center', fontweight='bold', color='#2c3e50')
    
    # Add context text (In vs Out) inside the bar
    in_v = bridges_pd.iloc[i]["in_degree"]
    out_v = bridges_pd.iloc[i]["out_degree"]
    context_text = f"In: {in_v} | Out: {out_v}"
    ax.text(2, i, context_text, va='center', color='white', fontweight='bold', fontsize=9)

ax.set_xlabel("Bridge Score (Geometric Mean of In-Degree & Out-Degree)", fontsize=12, fontweight="bold")
ax.set_title("Top 15 Evolutionary Bridges in Music History\nArtists who equally sample and are sampled", fontsize=15, fontweight="bold", pad=20)

plt.tight_layout()
plt.savefig(f"{output_dir}/fig9_top_bridges.pdf", bbox_inches="tight")
plt.savefig(f"{output_dir}/fig9_top_bridges.png", bbox_inches="tight")
print("  ✓ Saved fig9_top_bridges.pdf")
plt.close()

# =============================================================================
# EXPERIMENT 2: INSULAR COMMUNITY HYPOTHESIS
# =============================================================================
if df_labels is not None:
    print("\n[Experiment 2] Internal vs External Authority...")
    
    # Get top 15 authorities
    top_auth_pd = df_pagerank.orderBy(desc("authority_score")).toPandas()
    top_auth_pd = top_auth_pd[top_auth_pd["artist"].apply(is_readable)].head(15)
    top_15_names = top_auth_pd["artist"].tolist()
    
    # Map each artist to their cluster
    # music_labels has artist_name -> cluster_representative
    artist_clusters = df_labels.select("artist_name", "cluster_representative").dropDuplicates(["artist_name"])
    
    # Get all incoming edges for the top 15 authorities
    edges = df_graph.filter(col("Original_Artist_Name").isin(top_15_names)).select("Sampler_Artist_Name", "Original_Artist_Name")
    
    # Join to get cluster of Original Artist
    edges = edges.join(artist_clusters, edges.Original_Artist_Name == artist_clusters.artist_name, "left") \
                 .withColumnRenamed("cluster_representative", "original_cluster") \
                 .drop("artist_name")
                 
    # Join to get cluster of Sampler Artist
    edges = edges.join(artist_clusters, edges.Sampler_Artist_Name == artist_clusters.artist_name, "left") \
                 .withColumnRenamed("cluster_representative", "sampler_cluster") \
                 .drop("artist_name")
                 
    # Categorize as Internal or External
    edges = edges.withColumn("is_internal", when(col("original_cluster") == col("sampler_cluster"), 1).otherwise(0))
    
    stats = edges.groupBy("Original_Artist_Name").agg(
        count("*").alias("total_samples"),
        _sum("is_internal").alias("internal_samples")
    ).toPandas()
    
    stats["external_samples"] = stats["total_samples"] - stats["internal_samples"]
    stats["internal_pct"] = (stats["internal_samples"] / stats["total_samples"]) * 100
    stats["external_pct"] = (stats["external_samples"] / stats["total_samples"]) * 100
    
    # Sort stats to match top_15_names order
    stats.set_index("Original_Artist_Name", inplace=True)
    stats = stats.reindex(top_15_names)
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    y_pos = np.arange(len(top_15_names))
    
    p1 = ax.barh(y_pos, stats["internal_pct"], color="#E74C3C", edgecolor="white", height=0.7, label="Internal Influence (Same Community)")
    p2 = ax.barh(y_pos, stats["external_pct"], left=stats["internal_pct"], color="#3498DB", edgecolor="white", height=0.7, label="External Influence (Other Communities)")
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(top_15_names, fontweight="bold")
    ax.invert_yaxis()
    
    for i in range(len(top_15_names)):
        int_pct = stats.iloc[i]["internal_pct"]
        ext_pct = stats.iloc[i]["external_pct"]
        if int_pct > 10:
            ax.text(int_pct / 2, i, f"{int_pct:.1f}%", va='center', ha='center', color='white', fontweight='bold')
        if ext_pct > 10:
            ax.text(int_pct + ext_pct / 2, i, f"{ext_pct:.1f}%", va='center', ha='center', color='white', fontweight='bold')
            
    ax.axvline(x=50, color='gray', linestyle='--', alpha=0.7)
    
    ax.set_xlabel("Percentage of Incoming Samples", fontsize=12, fontweight="bold")
    ax.set_title("Authority Context: Internal vs External Influence\nAre top authorities mainstream icons or niche community leaders?", fontsize=15, fontweight="bold", pad=35)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.01), ncol=2, framealpha=1)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/fig10_authority_composition.pdf", bbox_inches="tight")
    plt.savefig(f"{output_dir}/fig10_authority_composition.png", bbox_inches="tight")
    print("  ✓ Saved fig10_authority_composition.pdf")
    plt.close()

# =============================================================================
# EXPERIMENT 3: MACROSCOPIC FLOW (INTER-COMMUNITY)
# =============================================================================
if df_labels is not None:
    print("\n[Experiment 3] Macroscopic Inter-Community Flow...")
    
    # 1. Identify semantic names for clusters (Top PageRank artist per cluster)
    df_cluster_auth = df_labels.join(df_pagerank, df_labels.artist_name == df_pagerank.artist, 'left')
    window = Window.partitionBy('cluster_representative').orderBy(desc('authority_score'))
    top_artist_per_cluster = df_cluster_auth.withColumn('rank', row_number().over(window)) \
                                            .filter(col('rank') == 1) \
                                            .select(col('cluster_representative'), col('artist').alias('cluster_name'))
                                            
    # Map each artist to their cluster NAME
    artist_cluster_names = artist_clusters.join(top_artist_per_cluster, "cluster_representative") \
                                          .select("artist_name", "cluster_name")
                                          
    # Get all edges
    edges = df_graph.filter((col("Sampler_Artist_Name") != "[unknown]") & (col("Original_Artist_Name") != "[unknown]")) \
                    .select("Sampler_Artist_Name", "Original_Artist_Name")
                    
    # Join cluster names
    edges = edges.join(artist_cluster_names, edges.Original_Artist_Name == artist_cluster_names.artist_name, "inner") \
                 .withColumnRenamed("cluster_name", "original_cluster") \
                 .drop("artist_name")
                 
    edges = edges.join(artist_cluster_names, edges.Sampler_Artist_Name == artist_cluster_names.artist_name, "inner") \
                 .withColumnRenamed("cluster_name", "sampler_cluster") \
                 .drop("artist_name")
                 
    # Filter out intra-cluster and count
    macro_flow = edges.filter(col("sampler_cluster") != col("original_cluster")) \
                      .groupBy("sampler_cluster", "original_cluster") \
                      .agg(count("*").alias("weight")) \
                      .orderBy(desc("weight")) \
                      .limit(15) \
                      .toPandas()
                      
    macro_flow = macro_flow[macro_flow["sampler_cluster"].apply(is_readable) & macro_flow["original_cluster"].apply(is_readable)]
    
    G_macro = nx.DiGraph()
    for _, row in macro_flow.iterrows():
        G_macro.add_edge(row["sampler_cluster"], row["original_cluster"], weight=row["weight"])
        
    fig, ax = plt.subplots(figsize=(16, 12))
    pos = nx.circular_layout(G_macro)
    
    edges_plot = G_macro.edges()
    weights = [G_macro[u][v]['weight'] for u, v in edges_plot]
    max_weight = max(weights) if weights else 1
    
    nx.draw_networkx_edges(
        G_macro, pos,
        edge_color="#7f8c8d",
        width=[(w / max_weight) * 10 for w in weights],
        alpha=0.6,
        arrows=True,
        arrowsize=20,
        arrowstyle="-|>",
        ax=ax
    )
    
    in_degrees = dict(G_macro.in_degree(weight='weight'))
    max_in = max([in_degrees.get(n, 0) for n in G_macro.nodes()] + [1])
    node_sizes = [(in_degrees.get(n, 0) / max_in) * 4000 + 1000 for n in G_macro.nodes()]
    
    nx.draw_networkx_nodes(
        G_macro, pos,
        node_size=node_sizes,
        node_color="#2ecc71",
        edgecolors="#27ae60",
        linewidths=2,
        ax=ax
    )
    
    for node, (x, y) in pos.items():
        ax.text(x, y, node, fontsize=10, fontweight="bold", ha="center", va="center",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#27ae60", alpha=0.9))
                
    edge_labels = {(u, v): f"{d['weight']}" for u, v, d in G_macro.edges(data=True)}
    nx.draw_networkx_edge_labels(
        G_macro, pos,
        edge_labels=edge_labels,
        font_color="#c0392b",
        font_size=9,
        font_weight="bold",
        label_pos=0.25, # Labels placed 25% away from source, so reciprocal edges do not overlap
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="none", alpha=0.7),
        ax=ax
    )
        
    ax.set_title("Macroscopic Sampling Flow Between Communities\nTop 15 Inter-Community Sampling Pipelines (Edge Label = Total Samples)", fontsize=15, fontweight="bold", pad=20)
    ax.axis("off")
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/fig11_macro_community_flow.pdf", bbox_inches="tight")
    plt.savefig(f"{output_dir}/fig11_macro_community_flow.png", bbox_inches="tight")
    print("  ✓ Saved fig11_macro_community_flow.pdf")
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
print("\n" + "=" * 80)
print("✓ ADVANCED EXPERIMENTS COMPLETE!")
print("=" * 80)
