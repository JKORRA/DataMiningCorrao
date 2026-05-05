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

# Ground Truth (Top 10 most sampled artists from WhoSampled or general consensus)
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
    "Madonna"
]

gt_df = pd.DataFrame({"artist": ground_truth, "ground_truth_rank": range(1, 11)})

# Fuzzy merge and calculate Spearman Rank Correlation
merged_data = []
matched_indices = set()

for i, gt_artist in enumerate(ground_truth):
    best_match = None
    best_score = 0
    best_idx = -1
    
    # Only search top 2000 for performance and relevance
    for idx, row in pdf_pagerank.head(2000).iterrows():
        if idx in matched_indices:
            continue
        
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
    correlation, p_value = spearmanr(merged["ground_truth_rank"], merged["system_rank"])
    print(f"\nSpearman Rank Correlation Coefficient: {correlation:.4f}")
    print(f"P-value: {p_value:.4f}")
else:
    print("\nNot enough matching artists to compute correlation.")

# Save the validation results
merged.to_csv("outputs/external_validation.csv", index=False)
print("✓ External validation results saved to: outputs/external_validation.csv")

spark.stop()
