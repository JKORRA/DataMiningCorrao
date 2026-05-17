# Project Directory - Source Code and Analysis

This directory contains all the source code, data processing scripts, and analysis pipelines for the Music Genealogy Project.

---

## 📁 Directory Structure

```
project/
├── src/                    # Core analysis scripts (9 files)
│   ├── data_preparation.py
│   ├── top_ranking.py
│   ├── compute_authority_manual.py
│   ├── cluster.py
│   ├── validation_metrics.py
│   ├── cluster_quality.py
│   ├── external_validation.py
│   ├── generate_report_figures.py
│   └── genealogy_visualizations.py
│
├── utils/                  # Utility scripts (3 files)
│   ├── analyze_selfloops.py
│   ├── visualize_cluster.py
│   └── generate_interactive_network.py
│
├── outputs/                # Analysis results (generated)
│   ├── music_graph.parquet/
│   ├── artist_pagerank.parquet/
│   ├── music_labels.parquet/
│   ├── validation_summary.csv/
│   ├── external_validation.csv
│   ├── interactive_genealogy.html
│   └── ... (11 files total)
│
├── figures/                # Generated visualizations
│   └── report_figures/    # All statistical plots and network diagrams
│
├── mbdump/                 # MusicBrainz raw data (input)
│   └── ... (140+ TSV files)
│
├── checkpoints/            # Spark checkpoints (generated)
├── checkpoints_clustering/ # Clustering checkpoints (generated)
│
├── venv/                   # Python virtual environment
│
├── run_pipeline.sh         # Main execution script
└── requirements.txt        # Python dependencies
```

---

## 🚀 Quick Start

```bash
# Activate virtual environment
source venv/bin/activate

# Run complete pipeline
./run_pipeline.sh

# Or run individual steps
python3 src/data_preparation.py
python3 src/compute_authority_manual.py
# ... etc
```

---

## 📄 Core Scripts Description

### Data Processing
- **data_preparation.py**
  - Loads MusicBrainz TSV files with manual schema
  - Multiplex network: weighted edges for sampling, remix, mashup
  - Temporal evolution: computes release year per recording
  - Robust entity resolution: uses GIDs to remove self-loops
  - Output: `outputs/music_graph.parquet/`

### Analysis Scripts
- **top_ranking.py**
  - Calculates in-degree and out-degree centrality
  - Identifies top sampled artists (volume metrics)
  
- **compute_authority_manual.py**
  - Artist-Level Weighted PageRank implementation
  - Fixes mass leakage by redistributing dangling node (sink) ranks
  - Convergence criterion (tolerance: 0.0001)
  - Output: `outputs/artist_pagerank.parquet/`
  
- **cluster.py** (4,664 bytes)
  - Custom Label Propagation Algorithm
  - 6 iterations to detect musical communities
  - Output: `outputs/music_labels.parquet/`, `outputs/music_clusters.csv/`

### Validation & Quality
- **validation_metrics.py** (8,723 bytes)
  - Network statistics (density, degrees, components)
  - Output: `outputs/validation_summary.csv/`
  
- **cluster_quality.py**
  - Intra-cluster edge fraction (simplified modularity) calculation
  - Cluster size distribution
  - Bridge analysis (inter-cluster connections)
  - Output: `outputs/cluster_quality_summary.csv/`

- **external_validation.py**
  - Validates PageRank scores against WhoSampled ground truth
  - Computes Spearman Rank Correlation Coefficient
  - Output: `outputs/external_validation.csv`

### Visualization
- **generate_report_figures.py** (23,159 bytes)
  - Generates statistical plots for report
  - Rankings, cluster analysis, power-law validation
  - Output: `figures/report_figures/` (PNG + PDF)
  
- **genealogy_visualizations.py** (21,864 bytes)
  - Generates network diagrams: top 15 sampling network, hub analysis
  - Output: `figures/report_figures/` (PNG + PDF)

### Utilities
- **analyze_selfloops.py**
  - Detects and analyzes self-loop patterns
  - Used for data quality investigation
  
- **visualize_cluster.py**
  - Visualizes individual cluster structure
  - Helpful for exploratory analysis

- **generate_interactive_network.py**
  - D3.js force-directed graph of the top artists
  - Output: `outputs/interactive_genealogy.html`

---

## 📊 Output Files

All analysis results are stored in `outputs/`:

| File | Size | Description |
|------|------|-------------|
| music_graph.parquet/ | ~15MB | Cleaned graph (58,621 song-level edges, 68,527 nodes, 23,503 unique artists) |
| artist_pagerank.parquet/ | ~2MB | PageRank scores for all artists |
| music_labels.parquet/ | ~8MB | Cluster assignments for each song |
| music_clusters.csv/ | ~250KB | Cluster sizes and members |
| validation_summary.csv/ | ~5KB | Network statistics summary |
| cluster_quality_summary.csv/ | ~3KB | Intra-cluster edge fraction and quality metrics |
| top_100_artists_pagerank.csv/ | ~8KB | Top 100 artists by authority |
| cluster_sizes.csv/ | ~200KB | Detailed cluster distribution |
| cluster_bridges.csv/ | ~50KB | Inter-cluster connections |
| external_validation.csv | ~1KB | Spearman correlation against ground truth |
| interactive_genealogy.html | ~3MB | Interactive D3.js Network Explorer |

---

## 🎨 Generated Figures

### Statistical Plots (`figures/report_figures/`)
1. **fig2_volume_vs_authority.png/pdf** - Top 20 comparison (in-degree vs PageRank)
2. **fig4_cluster_distribution.png/pdf** - Cluster size histogram
3. **fig6_top_artists_comparison.png/pdf** - Top 10 detailed comparison
4. **fig7_powerlaw_analysis.png/pdf** - Cumulative degree distribution (log-log)
5. **statistics_summary.txt** - Text statistics

### Network Diagrams (`figures/report_figures/`)
1. **fig1_top15_sampling_network.png/pdf** - Core sampling relationships among top 15
2. **fig7_hub_analysis.png/pdf** - Hub analysis: in-degree vs out-degree scatter
3. **fig9_top_bridges.png/pdf** - Top 15 evolutionary bridges
4. **fig10_authority_composition.png/pdf** - Internal vs external influence
5. **fig11_macro_community_flow.png/pdf** - Macroscopic inter-community sampling flow

All figures are 300 DPI, publication-quality.

---

## 🔧 Technical Details

### Dependencies
See `requirements.txt` for full list:
- Apache Spark 3.5.0 (PySpark)
- NetworkX 3.1
- Matplotlib 3.7.1
- Pandas 2.0.2
- NumPy 1.24.3

### Computational Requirements
- **RAM**: 8GB+ recommended
- **CPU**: Multi-core for Spark parallelization
- **Disk**: ~500MB for outputs (Parquet files)
- **Runtime**: ~8-10 minutes total pipeline

### Spark Configuration
- Checkpointing enabled (prevents StackOverflowError)
- Parquet format for columnar storage
- Lazy evaluation with action triggers

---

## 📝 Notes

1. **Reproducibility**: Delete `outputs/` and `figures/` to regenerate from scratch
2. **Self-loops**: Automatically removed in data_preparation.py
3. **Virtual Environment**: Pre-configured with all dependencies
4. **MusicBrainz Data**: Raw dumps in `mbdump/` (~2GB compressed)

---

## 🔍 Quality Assurance

All scripts include:
- ✅ Error handling
- ✅ Progress reporting
- ✅ Data validation checks
- ✅ Self-loop detection warnings
- ✅ Convergence monitoring

---

For complete project documentation, see `../README.md` in the parent directory.

**Last Updated**: 12 February 2026
