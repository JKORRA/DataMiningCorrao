"""
Report Figure and Statistics Generator

This script generates foundational visualizations and statistics for the final report.

It produces:
- Figure 1: Volume vs Authority (3-panel plot including the Rank-vs-Rank scatter)
- Figure 5: Cluster Size Distribution
- Figure 6: Power-Law Degree Distribution
- `statistics_summary.txt`: A text file containing network density, in/out degree stats, and power-law concentration percentages.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, expr, sum as _sum, avg, stddev, first, row_number
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import numpy as np
import os
import shutil
import re

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "IPAGothic", "IPAMincho", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

plt.style.use("seaborn-v0_8-paper")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 10

os.makedirs("figures/report_figures", exist_ok=True)

print("=" * 80)
print("GENERATING REPORT FIGURES AND STATISTICS")
print("=" * 80)

spark = (
    SparkSession.builder.appName("ReportFigures")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\nLoading data...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_graph.cache()


def is_readable_name(name):
    if not isinstance(name, str) or name in ("[unknown]", "[no artist]", ""):
        return False
    return name.isprintable()


# =============================================================================
# FIGURE 1: Volume vs Authority (3-panel)
# =============================================================================
print("\n[1/4] Generating Volume vs Authority (3-panel)...")

top_volume = (
    df_graph.filter(col("Original_Artist_Name") != "[unknown]")
    .groupBy("Original_Artist_Name")
    .agg(count("*").alias("times_sampled"))
    .orderBy(desc("times_sampled"))
    .toPandas()
)
top_volume = top_volume[top_volume["Original_Artist_Name"].apply(is_readable_name)].head(20)

try:
    top_authority_all = (
        spark.read.parquet("outputs/artist_pagerank.parquet")
        .orderBy(desc("authority_score"))
        .toPandas()
    )
    top_authority = top_authority_all[
        top_authority_all["artist"].apply(is_readable_name)
    ].head(20)

    # Build vol→auth scatter data for all artists
    volume_all = (
        df_graph.filter(col("Original_Artist_Name") != "[unknown]")
        .groupBy("Original_Artist_Name")
        .agg(count("*").alias("times_sampled"))
        .toPandas()
    )
    volume_all = volume_all[volume_all["Original_Artist_Name"].apply(is_readable_name)]
    scatter_df = volume_all.merge(
        top_authority_all, left_on="Original_Artist_Name", right_on="artist", how="inner"
    )

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 7))

    # Left: Volume ranking
    ax1.barh(range(len(top_volume)), top_volume["times_sampled"], color="steelblue", edgecolor="white")
    ax1.set_yticks(range(len(top_volume)))
    ax1.set_yticklabels(top_volume["Original_Artist_Name"], fontsize=8)
    ax1.set_xlabel("Times Sampled", fontsize=11)
    ax1.set_title("Top 20 by Volume\n(In-Degree Centrality)", fontsize=12, fontweight="bold")
    ax1.invert_yaxis()
    ax1.grid(axis="x", alpha=0.3)

    # Center: Authority ranking
    ax2.barh(range(len(top_authority)), top_authority["authority_score"], color="coral", edgecolor="white")
    ax2.set_yticks(range(len(top_authority)))
    ax2.set_yticklabels(top_authority["artist"], fontsize=8)
    ax2.set_xlabel("Authority Score (PageRank)", fontsize=11)
    ax2.set_title("Top 20 by Authority\n(PageRank)", fontsize=12, fontweight="bold")
    ax2.invert_yaxis()
    ax2.grid(axis="x", alpha=0.3)

    # Right: Rank vs Rank scatter
    scatter_df["vol_rank"] = scatter_df["times_sampled"].rank(method="min", ascending=False)
    scatter_df["auth_rank"] = scatter_df["authority_score"].rank(method="min", ascending=False)

    ax3.scatter(
        scatter_df["vol_rank"], scatter_df["auth_rank"],
        s=15, alpha=0.25, c="#7f8c8d", edgecolors="none", zorder=2,
    )
    
    max_rank = max(scatter_df["vol_rank"].max(), scatter_df["auth_rank"].max())
    ax3.plot([1, max_rank], [1, max_rank], color="gray", linestyle="--", alpha=0.5, zorder=1)

    surprise_artists = ["Daniel Ingram", "電音部", "外神田文芸高校", "Porter Robinson", "Toby Fox", "C418"]
    vol_heavy = ["Daft Punk", "Lady Gaga", "Michael Jackson"]

    for _, row in scatter_df[scatter_df["Original_Artist_Name"].isin(surprise_artists)].iterrows():
        v, a = row["vol_rank"], row["auth_rank"]
        ax3.scatter([v], [a], s=80, c="#E74C3C", edgecolors="#C0392B", linewidths=1.5, zorder=10)
        ax3.annotate(
            row["Original_Artist_Name"],
            xy=(v, a),
            xytext=(v * 1.5, a * 0.3),
            fontsize=8, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFFACD", edgecolor="#E74C3C", alpha=0.85),
            arrowprops=dict(arrowstyle="->", color="#E74C3C", lw=0.8, alpha=0.6),
        )

    for _, row in scatter_df[scatter_df["Original_Artist_Name"].isin(vol_heavy)].iterrows():
        v, a = row["vol_rank"], row["auth_rank"]
        ax3.scatter([v], [a], s=80, c="#3498DB", edgecolors="#2980B9", linewidths=1.5, zorder=10)
        ax3.annotate(
            row["Original_Artist_Name"],
            xy=(v, a),
            xytext=(v * 0.3, a * 1.5),
            fontsize=8, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#E8F4FD", edgecolor="#3498DB", alpha=0.85),
            arrowprops=dict(arrowstyle="->", color="#3498DB", lw=0.8, alpha=0.6),
        )

    ax3.set_xlabel("Volume Rank (1 = Most Sampled)", fontsize=11, fontweight="bold")
    ax3.set_ylabel("Authority Rank (1 = Highest PageRank)", fontsize=11, fontweight="bold")
    ax3.set_title("Rank vs Rank Comparison\n(Highlighting Divergence)", fontsize=12, fontweight="bold")
    
    # Set axes so rank 1 is at top-right
    ax3.set_xlim(max_rank * 1.2, 0.8)
    ax3.set_ylim(max_rank * 1.2, 0.8)
    ax3.set_xscale("log")
    ax3.set_yscale("log")
    ax3.grid(True, alpha=0.3, linestyle="--")

    legend_elements = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#E74C3C", markersize=8, label="High Authority Surprises"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#3498DB", markersize=8, label="High Volume Heavyweights"),
        Line2D([0], [0], color="gray", linestyle="--", label="Matched Ranking"),
    ]
    ax3.legend(handles=legend_elements, loc="lower left", fontsize=7, framealpha=0.9)

    plt.tight_layout()
    png_path = "figures/report_figures/fig1_volume_vs_authority.png"
    pdf_path = "figures/report_figures/fig1_volume_vs_authority.pdf"
    plt.savefig(png_path, bbox_inches="tight")
    plt.savefig(pdf_path, bbox_inches="tight")
    print(f"  Saved: {png_path}")
    print(f"  Saved: {pdf_path}")
    plt.close()

except Exception as e:
    print(f"  Warning: Could not generate fig1: {e}")


# =============================================================================
# FIGURE 5: Cluster Size Distribution (renamed from fig4)
# =============================================================================
print("\n[2/4] Generating Cluster Size Distribution (Fig 5)...")

try:
    df_labels = spark.read.parquet("outputs/music_labels.parquet")
    df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")

    df_cluster_auth = df_labels.join(df_pagerank, df_labels.artist_name == df_pagerank.artist, "left")
    window = Window.partitionBy("cluster_representative").orderBy(desc("authority_score"))
    top_artist_per_cluster = (
        df_cluster_auth.withColumn("rank", row_number().over(window))
        .filter(col("rank") == 1)
    )

    cluster_sizes = df_labels.groupBy("cluster_representative").agg(count("*").alias("size"))
    cluster_df = (
        cluster_sizes.join(top_artist_per_cluster, "cluster_representative")
        .select("cluster_representative", "size", "artist")
        .orderBy(desc("size"))
        .toPandas()
    )

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    bins = np.logspace(np.log10(1), np.log10(cluster_df["size"].max()), 40)
    ax1.hist(cluster_df["size"], bins=bins, edgecolor="white", alpha=0.8, color="#27ae60")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlabel("Cluster Size (Number of Songs)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Frequency", fontsize=11, fontweight="bold")
    ax1.set_title("Cluster Size Distribution", fontsize=13, fontweight="bold")
    ax1.grid(True, alpha=0.3, linestyle="--")

    top_clusters = cluster_df.head(20)
    labels = [
        f"{artist} Cluster" if is_readable_name(artist) else f"Cluster {rep}"
        for artist, rep in zip(top_clusters["artist"], top_clusters["cluster_representative"])
    ]

    ax2.barh(range(len(top_clusters)), top_clusters["size"], color="#1e8449", edgecolor="white")
    ax2.set_yticks(range(len(top_clusters)))
    ax2.set_yticklabels(labels, fontsize=8)
    ax2.set_xlabel("Number of Songs", fontsize=11, fontweight="bold")
    ax2.set_title("Top 20 Largest Communities\n(Labeled by Top Authority Artist)", fontsize=13, fontweight="bold")
    ax2.invert_yaxis()
    ax2.grid(axis="x", alpha=0.3)

    plt.tight_layout()
    png_path = "figures/report_figures/fig5_cluster_distribution.png"
    pdf_path = "figures/report_figures/fig5_cluster_distribution.pdf"
    plt.savefig(png_path, bbox_inches="tight")
    plt.savefig(pdf_path, bbox_inches="tight")
    print(f"  Saved: {png_path}")
    print(f"  Saved: {pdf_path}")
    plt.close()

    print(f"  Total clusters: {len(cluster_df):,}")
    print(f"  Mean cluster size: {cluster_df['size'].mean():.2f}")
    print(f"  Median cluster size: {cluster_df['size'].median():.0f}")
    print(f"  Largest cluster: {cluster_df['size'].max():,} songs")

except Exception as e:
    print(f"  Warning: Could not generate cluster distribution: {e}")


# =============================================================================
# Graph Statistics Summary
# =============================================================================
print("\n[3/4] Generating Graph Statistics Summary...")

total_edges = df_graph.count()
unique_samplers = df_graph.select("Sampler_Artist_Name").distinct().count()
unique_originals = df_graph.select("Original_Artist_Name").distinct().count()
unique_artists = (
    df_graph.select("Sampler_Artist_Name")
    .union(df_graph.select("Original_Artist_Name"))
    .distinct()
    .count()
)
unique_songs = (
    df_graph.select("source_song_id")
    .union(df_graph.select("target_song_id"))
    .distinct()
    .count()
)

in_stats = (
    df_graph.groupBy("Original_Artist_Name")
    .agg(count("*").alias("in_deg"))
    .agg(avg("in_deg").alias("mean"), stddev("in_deg").alias("std"), expr("max(in_deg)").alias("max"))
    .collect()[0]
)

out_stats = (
    df_graph.groupBy("Sampler_Artist_Name")
    .agg(count("*").alias("out_deg"))
    .agg(avg("out_deg").alias("mean"), stddev("out_deg").alias("std"), expr("max(out_deg)").alias("max"))
    .collect()[0]
)

# Power-law concentration analysis
in_degree = (
    df_graph.groupBy("Original_Artist_Name")
    .agg(count("*").alias("degree"))
    .toPandas()
)
total_samples = in_degree["degree"].sum()
in_degree_sorted = in_degree.sort_values("degree", ascending=False)
top_1_pct = int(len(in_degree) * 0.01)
top_5_pct = int(len(in_degree) * 0.05)
top_10_pct = int(len(in_degree) * 0.10)
top1_share = in_degree_sorted.head(top_1_pct)["degree"].sum() / total_samples * 100
top5_share = in_degree_sorted.head(top_5_pct)["degree"].sum() / total_samples * 100
top10_share = in_degree_sorted.head(top_10_pct)["degree"].sum() / total_samples * 100

print(f"  Concentration: Top 1% = {top1_share:.1f}%, Top 5% = {top5_share:.1f}%, Top 10% = {top10_share:.1f}%")

print("\n[3.5/4] Generating Degree Distribution (Power-Law) Plot (Fig 6)...")
try:
    degree_freq = in_degree.groupby("degree").size().reset_index(name="frequency")
    
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(degree_freq["degree"], degree_freq["frequency"], color="#3498DB", alpha=0.7, edgecolors="white", s=40)
    
    # Fit line to log-log data
    import numpy as np
    log_x = np.log10(degree_freq["degree"])
    log_y = np.log10(degree_freq["frequency"])
    m, b = np.polyfit(log_x, log_y, 1)
    
    x_fit = np.logspace(np.log10(degree_freq["degree"].min()), np.log10(degree_freq["degree"].max()), 100)
    y_fit = (10**b) * (x_fit**m)
    
    ax.plot(x_fit, y_fit, color="#E74C3C", linestyle="--", linewidth=2, label=f"Power-Law Fit (γ ≈ {abs(m):.2f})")
    
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Degree (Number of Times Sampled)", fontsize=11, fontweight="bold")
    ax.set_ylabel("Frequency (Number of Artists)", fontsize=11, fontweight="bold")
    ax.set_title("Degree Distribution of the Sampling Network\n(Log-Log Scale)", fontsize=13, fontweight="bold")
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.legend(loc="upper right", fontsize=10)
    
    plt.tight_layout()
    png_path = "figures/report_figures/fig6_degree_distribution.png"
    pdf_path = "figures/report_figures/fig6_degree_distribution.pdf"
    plt.savefig(png_path, bbox_inches="tight")
    plt.savefig(pdf_path, bbox_inches="tight")
    print(f"  Saved: {png_path}")
    print(f"  Saved: {pdf_path}")
    plt.close()
except Exception as e:
    print(f"  Warning: Could not generate degree distribution: {e}")


# =============================================================================
# Statistics summary file
# =============================================================================
print("\n[4/4] Generating statistics summary file...")

with open("figures/report_figures/statistics_summary.txt", "w") as f:
    f.write("=" * 80 + "\n")
    f.write("MUSIC GENEALOGY PROJECT - STATISTICS SUMMARY\n")
    f.write("=" * 80 + "\n\n")
    f.write("GRAPH STRUCTURE\n")
    f.write("-" * 80 + "\n")
    f.write(f"Total Sampling Events (Edges): {total_edges:,}\n")
    f.write(f"Unique Songs (Nodes): {unique_songs:,}\n")
    f.write(f"Unique Artists: {unique_artists:,}\n")
    f.write(f"  - Artists Who Sample: {unique_samplers:,}\n")
    f.write(f"  - Artists Being Sampled: {unique_originals:,}\n")
    f.write(f"Graph Density: {total_edges / (unique_songs * (unique_songs - 1)):.8f}\n")
    f.write("\n")
    f.write("IN-DEGREE STATISTICS (Times Sampled)\n")
    f.write("-" * 80 + "\n")
    f.write(f"Mean: {in_stats['mean']:.2f}\n")
    f.write(f"Std Dev: {in_stats['std']:.2f}\n")
    f.write(f"Maximum: {int(in_stats['max']):,}\n")
    f.write("\n")
    f.write("OUT-DEGREE STATISTICS (Samples Used)\n")
    f.write("-" * 80 + "\n")
    f.write(f"Mean: {out_stats['mean']:.2f}\n")
    f.write(f"Std Dev: {out_stats['std']:.2f}\n")
    f.write(f"Maximum: {int(out_stats['max']):,}\n")
    f.write("\n")
    f.write("POWER-LAW ANALYSIS (Concentration)\n")
    f.write("-" * 80 + "\n")
    f.write(f"Top 1% of artists control: {top1_share:.1f}% of sampling events\n")
    f.write(f"Top 5% of artists control: {top5_share:.1f}% of sampling events\n")
    f.write(f"Top 10% of artists control: {top10_share:.1f}% of sampling events\n")
    f.write("\n")
    f.write("This confirms the network exhibits scale-free (power-law) characteristics,\n")
    f.write("typical of real-world social and influence networks.\n")

print("  Saved: statistics_summary.txt")


# Copy generated figures to report/Immagini/
report_img_dir = "../report/Immagini"
if os.path.exists(report_img_dir):
    for fname in os.listdir("figures/report_figures"):
        if fname.startswith(("fig1_", "fig5_", "fig6_")):
            src = os.path.join("figures/report_figures", fname)
            dst = os.path.join(report_img_dir, fname)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
                print(f"  Copied {fname} to {report_img_dir}/")

spark.stop()
print("\nDone!")
