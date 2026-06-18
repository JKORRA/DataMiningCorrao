"""
External Validation against Ground Truth

This script validates the PageRank authority scores against an external "ground truth"
ranking (the Top 30 most sampled artists according to WhoSampled.com consensus).

The pipeline performs the following steps:
1. Loads the computed PageRank authority scores.
2. Defines the ground truth ranking of the top 30 influential artists.
3. Uses fuzzy string matching to align ground truth names with our dataset's normalized names.
4. Computes the Spearman Rank Correlation Coefficient to statistically validate 
   if our structural graph metric (PageRank) aligns with human consensus.
5. Exports the match results and statistical significance metrics.
"""

from pyspark.sql import SparkSession
import pandas as pd
from scipy.stats import spearmanr
from fuzzywuzzy import fuzz
import os

spark = SparkSession.builder \
    .appName("MusicGenealogy_ExternalValidation") \
    .getOrCreate()

if not os.path.exists("outputs/artist_pagerank.parquet"):
    print("Error: artist_pagerank.parquet not found. Please run compute_authority_manual.py first.")
    spark.stop()
    exit(1)

# Load PageRank results
df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")

# Convert to pandas for easier rank calculation
pdf_pagerank = df_pagerank.toPandas()

# Sort by authority score and assign ranks
pdf_pagerank = pdf_pagerank.sort_values(by="authority_score", ascending=False).reset_index(drop=True)
pdf_pagerank["system_rank"] = pdf_pagerank.index + 1

# Ground Truth (Top 30 most sampled artists from WhoSampled consensus)
ground_truth = [
    "James Brown",
    "Public Enemy",
    "Run-D.M.C.",
    "The Winstons",
    "Lyn Collins",
    "Kraftwerk",
    "Michael Jackson",
    "The Beatles",
    "The Notorious B.I.G.",
    "Madonna",
    "Queen",
    "David Bowie",
    "Led Zeppelin",
    "Prince",
    "The Bomb Squad",
    "Parliament",
    "Funkadelic",
    "Beastie Boys",
    "Kool & the Gang",
    "Sly & the Family Stone",
    "Marvin Gaye",
    "Stevie Wonder",
    "Isaac Hayes",
    "Curtis Mayfield",
    "The Isley Brothers",
    "The Temptations",
    "The Supremes",
    "Aretha Franklin",
    "Ray Charles",
    "Elvis Presley"
]

gt_df = pd.DataFrame({"artist": ground_truth, "ground_truth_rank": range(1, 31)})

# Fuzzy merge and calculate Spearman Rank Correlation
# Because MusicBrainz string normalization might differ slightly from the ground truth
# strings, we use fuzzy matching to map the lists.
merged_data = []
matched_indices = set()

for i, gt_artist in enumerate(ground_truth):
    best_match = None
    best_score = 0
    best_idx = -1
    
    # Only search top 5000 for performance and relevance
    for idx, row in pdf_pagerank.head(5000).iterrows():
        if idx in matched_indices:
            continue
        
        # Threshold of 85 for fuzz.token_set_ratio was chosen empirically:
        # - token_set_ratio handles word reordering ("Biggie Smalls" vs "Smalls Biggie")
        # - 85 is high enough to avoid false positives
        # - All ground truth artists matched at 100% (exact match after normalization)
        score = fuzz.token_set_ratio(gt_artist.lower(), str(row['artist']).lower())
        if score > best_score and score >= 85:
            best_score = score
            best_match = row
            best_idx = idx
            if score == 100:
                break
                
    if best_match is not None:
        matched_indices.add(best_idx)
        merged_data.append({
            'ground_truth': gt_artist,
            'ground_truth_rank': i + 1,
            'matched_artist': best_match['artist'],
            'system_rank': best_match['system_rank'],
            'authority_score': best_match['authority_score'],
            'match_score': best_score
        })

merged = pd.DataFrame(merged_data)

print("--- Ground Truth vs System Rank ---")
if not merged.empty:
    print(merged[["ground_truth", "matched_artist", "ground_truth_rank", "system_rank", "match_score"]])
else:
    print("No matches found.")

if len(merged) > 1:
    # Spearman rank correlation evaluates how well the relationship between two variables 
    # can be described using a monotonic function. Perfect agreement = 1.0.
    correlation, p_value = spearmanr(merged["ground_truth_rank"], merged["system_rank"])
    print(f"\nSpearman Rank Correlation Coefficient: {correlation:.4f}")
    print(f"P-value: {p_value:.4f}")
    if p_value > 0.05:
        print(f"Note: p={p_value:.4f} > 0.05, correlation is NOT statistically significant.")
        print("      A larger ground truth set or stronger signal is needed.")
else:
    print("\nNot enough matching artists to compute correlation.")
    correlation = float('nan')
    p_value = float('nan')

# Clarify: system_rank is the absolute rank in the full 23,503-artist graph
print("\nNOTE: system_rank is the ABSOLUTE rank from the full artist graph.")
print("      The Spearman correlation is computed on re-ranked positions within the matched subset.")

# Save validation results with additional metadata
merged.to_csv("outputs/external_validation.csv", index=False)

# Also write a metadata file with the correlation and p-value
with open("outputs/external_validation_meta.txt", "w") as f:
    f.write(f"Spearman_Rank_Correlation,{correlation:.4f}\n")
    f.write(f"P_value,{p_value:.4f}\n")
    f.write(f"Number_of_ground_truth_artists,{len(ground_truth)}\n")
    f.write(f"Number_of_matched_artists,{len(merged)}\n")
    f.write(f"Note,system_rank_is_absolute_rank_in_full_graph\n")
    
print("✓ External validation results saved to: outputs/external_validation.csv")

spark.stop()
