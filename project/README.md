# Project Directory - Source Code and Analysis

This directory contains all the source code, data processing scripts, and analysis pipelines for the Music Genealogy Project.

---

## 📁 Directory Structure

```
project/
├── src/                    # Core analysis scripts (10 files)
│   ├── 01_build_graph.py
│   ├── 02_degree_centrality.py
│   ├── 03_page_rank.py
│   ├── 04_louvain_clustering.py
│   ├── 05_network_statistics.py
│   ├── 06_cluster_evaluation.py
│   ├── 07_plot_hubs_bridges.py
│   ├── 08_authority_context.py
│   ├── 09_generate_figures.py
│   └── 10_ground_truth_validation.py
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
├── .venv/                   # Python virtual environment
│
├── run_pipeline.sh         # Main execution script
└── requirements.txt        # Python dependencies
```

---

## 🚀 Quick Start

```bash
# Activate virtual environment
source .venv/bin/activate

# Run complete pipeline
./run_pipeline.sh

# Or run individual steps
python3 src/01_build_graph.py
python3 src/03_page_rank.py
# ... etc
```

---

## 📄 Core Scripts Description

### Data Processing
- **01_build_graph.py**
  - Loads MusicBrainz TSV files with manual schema
  - Multiplex network: weighted edges for sampling, remix, mashup
  - Temporal evolution: computes release year per recording
  - Robust entity resolution: uses GIDs to remove self-loops
  - Output: `outputs/music_graph.parquet/`

### Analysis Scripts
- **02_degree_centrality.py**
  - Calculates in-degree and out-degree centrality
  - Identifies top sampled artists (volume metrics)
  
- **03_page_rank.py**
  - Artist-Level Weighted PageRank implementation
  - Fixes mass leakage by redistributing dangling node (sink) ranks
  - Convergence criterion (tolerance: 0.0001)
  - Output: `outputs/artist_pagerank.parquet/`
  
- **04_louvain_clustering.py**
  - Louvain community detection via NetworkX (resolution r=1.0)
  - Constructs weighted undirected graph from directed sampling edges
  - Output: `outputs/music_clusters.csv`, `outputs/music_cluster_membership.csv`

### Validation & Quality
- **05_network_statistics.py**
  - Network statistics (density, degrees, components)
  - Output: `outputs/validation_summary.csv/`
  
- **06_cluster_evaluation.py**
  - Intra-cluster edge fraction (simplified modularity) calculation
  - Cluster size distribution
  - Bridge analysis (inter-cluster connections)
  - Output: `outputs/cluster_quality_summary.csv/`

- **10_ground_truth_validation.py**
  - Validates PageRank scores against WhoSampled ground truth
  - Computes Spearman Rank Correlation Coefficient
  - Output: `outputs/external_validation.csv`

### Visualization
- **09_generate_figures.py**
  - Generates Fig 1 (Volume vs Authority, 3-panel) and Fig 5 (Cluster Distribution)
  - Output: `figures/report_figures/` (PNG + PDF)
  
- **07_plot_hubs_bridges.py**
  - Generates Fig 2: merged hub analysis + top bridges
  - Output: `figures/report_figures/` (PNG + PDF)
  
- **08_authority_context.py**
  - Generates Fig 3: Authority Context (Internal vs External Influence)
  - Output: `figures/report_figures/` (PNG + PDF)

### Utilities
- **analyze_selfloops.py**
  - Detects and analyzes self-loop patterns
  - Used for data quality investigation
  
- **visualize_cluster.py**
  - Generates Fig 4: cluster-colored artist network (top 50 by PageRank)
  - Light theme, intra/inter edge distinction, cluster legend
  - Output: `figures/report_figures/` (PNG + PDF)

- **generate_interactive_network.py**
  - D3.js force-directed graph of the top artists
  - Output: `outputs/interactive_genealogy.html`

---

## 📊 Output Files

All analysis results are stored in `outputs/`:

| File | Size | Description |
|------|------|-------------|
| music_graph.parquet/ | ~15MB | Cleaned graph (57,741 song-level edges, 67,638 nodes, 23,242 unique artists) |
| artist_pagerank.parquet/ | ~2MB | PageRank scores for all artists |
| music_labels.parquet | ~368KB | Community assignments for each song |
| music_clusters.csv | ~1.5KB | Community statistics (1,774 clusters) |
| music_cluster_membership.csv | ~2MB | Artist-to-community mapping |
| validation_summary.csv/ | ~5KB | Network statistics summary |
| cluster_quality_summary.csv/ | ~3KB | Intra-cluster edge fraction and quality metrics |
| top_100_artists_pagerank.csv/ | ~8KB | Top 100 artists by authority |
| cluster_sizes.csv/ | ~200KB | Detailed cluster distribution |
| cluster_bridges.csv/ | ~50KB | Inter-cluster connections |
| external_validation.csv | ~1KB | Spearman correlation against ground truth |
| interactive_genealogy.html | ~3MB | Interactive D3.js Network Explorer |

---

## 🎨 Generated Figures

### Report Figures (`figures/report_figures/`)
1. **fig1_volume_vs_authority.png/pdf** - 3-panel: top 20 by volume, top 20 by authority, scatter with surprise artists
2. **fig2_hub_bridges.png/pdf** - Merged hub analysis + top 15 evolutionary bridges (cluster-colored)
3. **fig3_authority_context.png/pdf** - Internal vs external influence for top 15 authorities
4. **fig4_cluster_artist_network.png/pdf** - Top 50 artists by PageRank, colored by cluster
5. **fig5_cluster_distribution.png/pdf** - Cluster size histogram & top 20 communities
6. **statistics_summary.txt** - Text statistics

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
2. **Self-loops**: Automatically removed in 01_build_graph.py
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

**Last Updated**: 20 May 2026
