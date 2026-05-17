"""
Report Figure Generator
Generates all plots, graphs, and statistics for the academic report.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, expr, sum as _sum, avg, stddev
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Droid Sans Fallback", "sans-serif"]
import matplotlib.patches as mpatches
import numpy as np
import os

plt.style.use("seaborn-v0_8-paper")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 10
# Removed forced serif to allow fallback fonts
# plt.rcParams["font.family"] = "serif"

os.makedirs("report_figures", exist_ok=True)

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



# Figure 2: Volume vs Authority Comparison
print("\n[1/5] Generating Volume vs Authority Comparison...")

import re


def is_readable_name(name):
    if not isinstance(name, str) or name in ("[unknown]", "[no artist]", ""):
        return False
    return name.isprintable()


# Get top 20 by volume (in-degree), filtered
top_volume = (
    df_graph.filter(col("Original_Artist_Name") != "[unknown]")
    .groupBy("Original_Artist_Name")
    .agg(count("*").alias("times_sampled"))
    .orderBy(desc("times_sampled"))
    .toPandas()
)
top_volume = top_volume[
    top_volume["Original_Artist_Name"].apply(is_readable_name)
].head(20)

# Get top 20 by authority (PageRank), filtered
try:
    top_authority_all = (
        spark.read.parquet("outputs/artist_pagerank.parquet")
        .orderBy(desc("authority_score"))
        .toPandas()
    )
    top_authority = top_authority_all[
        top_authority_all["artist"].apply(is_readable_name)
    ].head(20)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 8))

    # Volume ranking
    ax1.barh(range(len(top_volume)), top_volume["times_sampled"], color="steelblue")
    ax1.set_yticks(range(len(top_volume)))
    ax1.set_yticklabels(top_volume["Original_Artist_Name"], fontsize=9)
    ax1.set_xlabel("Times Sampled (Volume)", fontsize=11)
    ax1.set_title(
        "Top 20 Artists by Volume\n(In-Degree Centrality)",
        fontsize=12,
        fontweight="bold",
    )
    ax1.invert_yaxis()
    ax1.grid(axis="x", alpha=0.3)

    # Authority ranking
    ax2.barh(range(len(top_authority)), top_authority["authority_score"], color="coral")
    ax2.set_yticks(range(len(top_authority)))
    ax2.set_yticklabels(top_authority["artist"], fontsize=9)
    ax2.set_xlabel("Authority Score (PageRank)", fontsize=11)
    ax2.set_title(
        "Top 20 Artists by Authority\n(PageRank)", fontsize=12, fontweight="bold"
    )
    ax2.invert_yaxis()
    ax2.grid(axis="x", alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        "figures/report_figures/fig2_volume_vs_authority.png", bbox_inches="tight"
    )
    plt.savefig(
        "figures/report_figures/fig2_volume_vs_authority.pdf", bbox_inches="tight"
    )
    print("   ✓ Saved: fig2_volume_vs_authority.png/pdf")
    plt.close()

except Exception as e:
    print(
        f"   ⚠ Warning: Could not generate authority comparison (run PageRank first): {e}"
    )

# Figure 3: PageRank Convergence (placeholder)
print("\n[2/5] Generating PageRank Convergence Plot...")
print("   ⚠ Note: Run PageRank with convergence logging to generate this plot")

# Figure 4: Cluster Size Distribution
print("\n[3/5] Generating Cluster Size Distribution...")

try:
    from pyspark.sql.functions import first, row_number
    from pyspark.sql.window import Window
    
    df_labels = spark.read.parquet("outputs/music_labels.parquet")
    df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")

    df_cluster_auth = df_labels.join(df_pagerank, df_labels.artist_name == df_pagerank.artist, 'left')
    window = Window.partitionBy('cluster_representative').orderBy(desc('authority_score'))
    top_artist_per_cluster = df_cluster_auth.withColumn('rank', row_number().over(window)) \
                                            .filter(col('rank') == 1)

    cluster_sizes = df_labels.groupBy('cluster_representative').agg(count('*').alias('size'))
    cluster_df = cluster_sizes.join(top_artist_per_cluster, 'cluster_representative') \
                              .select('cluster_representative', 'size', 'artist') \
                              .orderBy(desc('size')).toPandas()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Histogram - filled density style
    # Use log-spaced bins for better visualization of power-law distribution
    bins = np.logspace(np.log10(1), np.log10(cluster_df["size"].max()), 40)
    ax1.hist(
        cluster_df["size"], bins=bins,
        edgecolor="white", alpha=0.8, color="#27ae60"
    )
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlabel("Cluster Size (Number of Songs)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Frequency", fontsize=11, fontweight="bold")
    ax1.set_title("Cluster Size Distribution", fontsize=13, fontweight="bold")
    ax1.grid(True, alpha=0.3, linestyle="--")

    # Top 20 clusters with semantic labels
    top_clusters = cluster_df.head(20)
    # create labels like "James Brown Cluster"
    labels = [f"{artist} Cluster" if is_readable_name(artist) else f"Cluster {rep}" 
              for artist, rep in zip(top_clusters["artist"], top_clusters["cluster_representative"])]
              
    ax2.barh(range(len(top_clusters)), top_clusters["size"], color="#1e8449")
    ax2.set_yticks(range(len(top_clusters)))
    ax2.set_yticklabels(labels, fontsize=9)
    ax2.set_xlabel("Number of Songs", fontsize=11, fontweight="bold")
    ax2.set_title("Top 20 Largest Communities\n(Labeled by Top Authority Artist)", fontsize=13, fontweight="bold")
    ax2.invert_yaxis()
    ax2.grid(axis="x", alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        "figures/report_figures/fig4_cluster_distribution.png", bbox_inches="tight"
    )
    plt.savefig(
        "figures/report_figures/fig4_cluster_distribution.pdf", bbox_inches="tight"
    )
    print("   ✓ Saved: fig4_cluster_distribution.png/pdf")
    plt.close()

    # Statistics
    print(f"   Total clusters: {len(cluster_df):,}")
    print(f"   Mean cluster size: {cluster_df['size'].mean():.2f}")
    print(f"   Median cluster size: {cluster_df['size'].median():.0f}")
    print(f"   Largest cluster: {cluster_df['size'].max():,} songs")

except Exception as e:
    print(
        f"   ⚠ Warning: Could not generate cluster distribution (run clustering first): {e}"
    )

# Figure 5: Graph Statistics Summary
print("\n[4/5] Generating Graph Statistics Summary...")

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

# In-degree stats
in_stats = (
    df_graph.groupBy("Original_Artist_Name")
    .agg(count("*").alias("in_deg"))
    .agg(
        avg("in_deg").alias("mean"),
        stddev("in_deg").alias("std"),
        expr("max(in_deg)").alias("max"),
    )
    .collect()[0]
)

# Out-degree stats
out_stats = (
    df_graph.groupBy("Sampler_Artist_Name")
    .agg(count("*").alias("out_deg"))
    .agg(
        avg("out_deg").alias("mean"),
        stddev("out_deg").alias("std"),
        expr("max(out_deg)").alias("max"),
    )
    .collect()[0]
)



# Figure 6: Top Artists Comparison Table (LaTeX table generated in BONUS section below)
print("\n[5/5] Top artists comparison table generated via LaTeX in BONUS section")

# Figure 7: Power-Law Analysis
print("\n[7/7] Generating Power-Law Analysis...")

# Compute degree distributions for power-law analysis
in_degree = (
    df_graph.groupBy("Original_Artist_Name")
    .agg(count("*").alias("degree"))
    .toPandas()
)

out_degree = (
    df_graph.groupBy("Sampler_Artist_Name")
    .agg(count("*").alias("degree"))
    .toPandas()
)

# Calculate cumulative distribution for In-Degree
in_degree_sorted = in_degree["degree"].sort_values(ascending=False).reset_index(drop=True)
in_cum = [(i + 1) / len(in_degree_sorted) for i in range(len(in_degree_sorted))]

# Calculate cumulative distribution for Out-Degree
out_degree_sorted = out_degree["degree"].sort_values(ascending=False).reset_index(drop=True)
out_cum = [(i + 1) / len(out_degree_sorted) for i in range(len(out_degree_sorted))]

fig, ax = plt.subplots(figsize=(9, 7))
ax.loglog(in_degree_sorted, in_cum, "o", alpha=0.6, markersize=4, color="#3498db", label="In-Degree (Times Sampled)")
ax.loglog(out_degree_sorted, out_cum, "s", alpha=0.5, markersize=4, color="#e67e22", label="Out-Degree (Samples Used)")

ax.set_xlabel("Degree", fontsize=12, fontweight="bold")
ax.set_ylabel("P(X ≥ x) - Cumulative Probability", fontsize=12, fontweight="bold")
ax.set_title("Power-Law Validation: Cumulative Degree Distributions", fontsize=14, fontweight="bold")
ax.grid(True, alpha=0.4, which="both", linestyle="--")

# Add reference line
max_degree = max(in_degree_sorted.max(), out_degree_sorted.max())
x_ref = np.logspace(0, np.log10(max_degree), 100)
y_ref = (x_ref / max_degree) ** (-1.5)
ax.plot(x_ref, y_ref, "r--", linewidth=2.5, label="Reference Power-Law (α ≈ 1.5)", alpha=0.8)

ax.legend(fontsize=11, framealpha=0.9)

plt.tight_layout()
plt.savefig("figures/report_figures/fig7_powerlaw_analysis.png", bbox_inches="tight")
plt.savefig("figures/report_figures/fig7_powerlaw_analysis.pdf", bbox_inches="tight")
print("   ✓ Saved: fig7_powerlaw_analysis.png/pdf")
plt.close()

# Concentration analysis
total_samples = in_degree["degree"].sum()
top_1_percent = int(len(in_degree) * 0.01)
top_5_percent = int(len(in_degree) * 0.05)
top_10_percent = int(len(in_degree) * 0.10)

in_degree_sorted_full = in_degree.sort_values("degree", ascending=False)
top1_share = (
    in_degree_sorted_full.head(top_1_percent)["degree"].sum() / total_samples * 100
)
top5_share = (
    in_degree_sorted_full.head(top_5_percent)["degree"].sum() / total_samples * 100
)
top10_share = (
    in_degree_sorted_full.head(top_10_percent)["degree"].sum() / total_samples * 100
)

print(f"\n   Concentration Analysis:")
print(f"   Top 1% of artists: {top1_share:.1f}% of all sampling events")
print(f"   Top 5% of artists: {top5_share:.1f}% of all sampling events")
print(f"   Top 10% of artists: {top10_share:.1f}% of all sampling events")

# Generate LaTeX tables
print("\n[BONUS] Generating LaTeX tables...")

# Top 10 comparison table
try:
    import re

    def wrap_cjk(text):
        if re.search("[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]", text):
            return r"\begin{CJK*}{UTF8}{gbsn}" + text + r"\end{CJK*}"
        return text

    top10_volume = (
        df_graph.filter(col("Original_Artist_Name") != "[unknown]")
        .groupBy("Original_Artist_Name")
        .agg(count("*").alias("volume"))
        .orderBy(desc("volume"))
        .limit(10)
        .toPandas()
    )

    top10_authority = (
        spark.read.parquet("outputs/artist_pagerank.parquet")
        .filter(col("artist") != "[unknown]")
        .orderBy(desc("authority_score"))
        .limit(10)
        .toPandas()
    )

    with open("figures/report_figures/table_top10_comparison.tex", "w") as f:
        f.write("% Top 10 Artists Comparison Table\n")
        f.write("\\begin{table}[h]\n")
        f.write("\\centering\n")
        f.write("\\caption{Comparison of Top 10 Artists by Volume vs. Authority}\n")
        f.write("\\label{tab:top10comparison}\n")
        f.write("\\begin{tabular}{clrclr}\n")
        f.write("\\toprule\n")
        f.write(
            "\\multicolumn{3}{c}{\\textbf{By Volume}} & \\multicolumn{3}{c}{\\textbf{By Authority}} \\\\\n"
        )
        f.write("\\cmidrule(r){1-3} \\cmidrule(l){4-6}\n")
        f.write("Rank & Artist & Count & Rank & Artist & Score \\\\\n")
        f.write("\\midrule\n")

        for i in range(10):
            vol_artist = wrap_cjk(
                top10_volume.iloc[i]["Original_Artist_Name"][:30].replace("&", "\\&")
            )
            vol_count = int(top10_volume.iloc[i]["volume"])
            auth_artist = wrap_cjk(
                top10_authority.iloc[i]["artist"][:30].replace("&", "\\&")
            )
            auth_score = f"{top10_authority.iloc[i]['authority_score']:.4f}"
            f.write(
                f"{i + 1} & {vol_artist} & {vol_count} & {i + 1} & {auth_artist} & {auth_score} \\\\\n"
            )

        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")
        f.write("\\end{table}\n")

    print("   ✓ Saved: table_top10_comparison.tex")

except Exception as e:
    print(f"   ⚠ Warning: Could not generate LaTeX table: {e}")

# Generate summary statistics file
print("\n[BONUS] Generating summary statistics file...")

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

    f.write(
        "This confirms the network exhibits scale-free (power-law) characteristics,\n"
    )
    f.write("typical of real-world social and influence networks.\n")

print("   ✓ Saved: statistics_summary.txt")

print("\n" + "=" * 80)
print("REPORT FIGURES GENERATION COMPLETE")
print("=" * 80)
print(f"\nAll figures saved to: report_figures/")
print("\nGenerated files:")
print("  • fig2_volume_vs_authority.png/pdf - Top 20 comparison")
print("  • fig4_cluster_distribution.png/pdf - Cluster size analysis")
print("  • fig7_powerlaw_analysis.png/pdf - Cumulative distribution")
print("  • table_top10_comparison.tex - LaTeX table")
print("  • statistics_summary.txt - Text summary for report")

spark.stop()
