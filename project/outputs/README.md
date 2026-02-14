# Analysis Outputs Directory

This directory contains the generated analysis results.

## ⚠️ Not Included in Git Repository

Generated files are **excluded from version control** as they can be reproduced by running the pipeline.

## 📊 Generated Files

After running `../run_pipeline.sh`, this directory will contain:

### Graph Data (~5 MB)
- **`music_graph.parquet/`** - Processed sampling network
  - 20,728 edges (after self-loop removal)
  - 30,021 songs
  - 13,587 unique artists
  - Format: Parquet (columnar)

### Authority Scores (~2 MB)
- **`artist_pagerank.parquet/`** - PageRank scores for all artists
  - Convergence: 11 iterations
  - Damping factor: 0.85
  - Tolerance: 0.0001
  
- **`top_100_artists_pagerank.csv/`** - Top 100 ranked artists
  - Human-readable CSV format
  - Columns: artist, authority_score, rank

### Clustering Results (~4 MB)
- **`music_labels.parquet/`** - Song-to-cluster assignments
  - 7,954 musical communities detected
  - Algorithm: Label Propagation (6 iterations)
  
- **`music_clusters.csv/`** - Cluster summary
  - Columns: cluster_id, song_count
  - Statistics: sizes, members

### Quality Metrics (~200 KB)
- **`validation_summary.csv/`** - Network statistics
  - Degree distributions
  - Density, diameter, components
  - Power-law concentration
  
- **`cluster_quality_summary.csv/`** - Modularity and cluster quality
  - Modularity: 0.2724
  - Average cluster size: 2.49
  - Largest cluster: 143 songs
  
- **`cluster_sizes.csv/`** - Detailed cluster size distribution

- **`cluster_bridges.csv/`** - Inter-cluster connections
  - Edge count between communities
  - Bridge analysis

## 📈 File Sizes (Approximate)

Total output size: **~15-20 MB**

```
music_graph.parquet/              ~5 MB
artist_pagerank.parquet/          ~2 MB
music_labels.parquet/             ~3 MB
top_100_artists_pagerank.csv/     ~10 KB
music_clusters.csv/               ~250 KB
validation_summary.csv/           ~5 KB
cluster_quality_summary.csv/      ~3 KB
cluster_sizes.csv/                ~200 KB
cluster_bridges.csv/              ~50 KB
```

## 🔄 How to Regenerate

From the `dataset/` directory:

### Complete Pipeline (8-10 minutes)
```bash
source venv/bin/activate
./run_pipeline.sh
```

### Individual Steps

```bash
# 1. Build graph (2-3 min)
python3 src/data_preparation.py
# Output: music_graph.parquet

# 2. Calculate authority (3-4 min)
python3 src/compute_authority_manual.py
# Output: artist_pagerank.parquet, top_100_artists_pagerank.csv

# 3. Detect communities (2 min)
python3 src/cluster.py
# Output: music_labels.parquet, music_clusters.csv

# 4. Compute validation metrics (1 min)
python3 src/validation_metrics.py
# Output: validation_summary.csv

# 5. Evaluate cluster quality (1 min)
python3 src/cluster_quality.py
# Output: cluster_quality_summary.csv, cluster_sizes.csv, cluster_bridges.csv
```

## 🔍 Viewing Results

### Parquet Files (binary format)

Use Python to read:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("view_results").getOrCreate()

# View graph
df_graph = spark.read.parquet("outputs/music_graph.parquet")
df_graph.show(10)

# View PageRank
df_pagerank = spark.read.parquet("outputs/artist_pagerank.parquet")
df_pagerank.orderBy("authority_score", ascending=False).show(10)
```

### CSV Files (text format)

Use standard tools:
```bash
# Top 10 artists
head outputs/top_100_artists_pagerank.csv/part-*.csv

# Network statistics
cat outputs/validation_summary.csv/part-*.csv

# Cluster quality
cat outputs/cluster_quality_summary.csv/part-*.csv
```

Or use pandas:
```python
import pandas as pd

# Read CSV outputs (Spark writes to directories)
import glob
files = glob.glob("outputs/top_100_artists_pagerank.csv/part-*.csv")
df = pd.concat([pd.read_csv(f) for f in files])
print(df.head(10))
```

## 📊 Expected Results

### Key Statistics

After successful pipeline execution:

**Network:**
- Nodes: 30,021 songs
- Edges: 20,728 sampling events
- Artists: 13,587
- Density: 0.00002456 (sparse)

**Authority (Top 3):**
1. James Brown - 11.70
2. Beastie Boys - 9.19
3. Daft Punk - 7.53

**Communities:**
- Total clusters: 7,954
- Modularity: 0.2724
- Largest: 143 songs
- Mean size: 2.49

**Convergence:**
- PageRank: 11 iterations
- Label Propagation: 6 iterations

## ⚙️ File Formats

### Parquet
- Binary columnar format
- Compressed (smaller than CSV)
- Fast to read/write
- Preserves data types
- Requires Spark or pandas to read

### CSV (Spark Output)
- Written to directories (e.g., `file.csv/`)
- Contains `part-*.csv` files (one per partition)
- May have `_SUCCESS` marker file
- No headers in individual parts

## 🗑️ Cleaning Outputs

To regenerate from scratch:

```bash
# Remove all outputs
rm -rf outputs/*

# Re-run pipeline
./run_pipeline.sh
```

To remove only specific outputs:
```bash
# Remove graph (forces full rebuild)
rm -rf outputs/music_graph.parquet

# Remove PageRank (recalculate authority)
rm -rf outputs/artist_pagerank.parquet outputs/top_100_artists_pagerank.csv

# Remove clustering
rm -rf outputs/music_labels.parquet outputs/music_clusters.csv
```

## 📝 Notes

1. **Deterministic Results**: Random seeds are fixed (seed=42), so results are reproducible.

2. **Incremental Execution**: Pipeline skips steps if outputs exist. Delete outputs to force re-run.

3. **Disk Space**: Keep ~50 MB free for outputs + ~500 MB for Spark temporary files.

4. **Spark Checkpoints**: Large temporary files in `../checkpoints/` can be deleted after successful runs.

## 🆘 Troubleshooting

**"FileNotFoundError: music_graph.parquet"**
```bash
# Graph not built yet
python3 src/data_preparation.py
```

**"Corrupt Parquet file"**
```bash
# Interrupted write - remove and rebuild
rm -rf outputs/music_graph.parquet
python3 src/data_preparation.py
```

**"Permission denied"**
```bash
# Fix permissions
chmod -R u+w outputs/
```

---

For more information, see `../README.md` or the main project documentation.
