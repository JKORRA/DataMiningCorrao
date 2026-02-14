# Project Directory - Source Code and Analysis

This directory contains all the source code, data processing scripts, and analysis pipelines for the Music Genealogy Project.

---

## 📁 Directory Structure

```
project/
├── src/                    # Core analysis scripts (8 files)
│   ├── data_preparation.py
│   ├── top_ranking.py
│   ├── compute_authority_manual.py
│   ├── cluster.py
│   ├── validation_metrics.py
│   ├── cluster_quality.py
│   ├── generate_report_figures.py
│   └── genealogy_visualizations.py
│
├── utils/                  # Utility scripts (2 files)
│   ├── analyze_selfloops.py
│   └── visualize_cluster.py
│
├── outputs/                # Analysis results (generated)
│   ├── music_graph.parquet/
│   ├── artist_pagerank.parquet/
│   ├── music_labels.parquet/
│   ├── validation_summary.csv/
│   ├── cluster_quality_summary.csv/
│   └── ... (9 files total)
│
├── figures/                # Generated visualizations
│   ├── report_figures/    # 8 statistical plots
│   └── genealogy_networks/ # 5 network diagrams
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
- **data_preparation.py** (5,251 bytes)
  - Loads MusicBrainz TSV files with manual schema
  - Filters for pure "sampling" relationships (IDs: 69, 231)
  - Removes self-loops (1,407 edges)
  - Performs multi-table joins to construct graph
  - Output: `outputs/music_graph.parquet/` (20,728 edges)

### Analysis Scripts
- **top_ranking.py** (1,868 bytes)
  - Calculates in-degree and out-degree centrality
  - Identifies top sampled artists (volume metrics)
  
- **compute_authority_manual.py** (5,713 bytes)
  - Custom PageRank implementation (iterative MapReduce)
  - Handles dangling nodes, lineage truncation
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
  
- **cluster_quality.py** (9,875 bytes)
  - Modularity calculation
  - Cluster size distribution
  - Bridge analysis (inter-cluster connections)
  - Output: `outputs/cluster_quality_summary.csv/`

### Visualization
- **generate_report_figures.py** (23,159 bytes)
  - Generates 8 statistical plots for report
  - Degree distributions, rankings, cluster analysis
  - Output: `figures/report_figures/` (PNG + PDF)
  
- **genealogy_visualizations.py** (21,864 bytes)
  - Generates 5 network diagrams
  - Sampling networks, ego networks, hub analysis
  - Output: `figures/genealogy_networks/` (PNG + PDF)

### Utilities
- **analyze_selfloops.py** (5,011 bytes)
  - Detects and analyzes self-loop patterns
  - Used for data quality investigation
  
- **visualize_cluster.py** (5,785 bytes)
  - Visualizes individual cluster structure
  - Helpful for exploratory analysis

---

## 📊 Output Files

All analysis results are stored in `outputs/`:

| File | Size | Description |
|------|------|-------------|
| music_graph.parquet/ | ~15MB | Cleaned graph (20,728 edges, 30,021 nodes) |
| artist_pagerank.parquet/ | ~2MB | PageRank scores for all artists |
| music_labels.parquet/ | ~8MB | Cluster assignments for each song |
| music_clusters.csv/ | ~250KB | Cluster sizes and members |
| validation_summary.csv/ | ~5KB | Network statistics summary |
| cluster_quality_summary.csv/ | ~3KB | Modularity and quality metrics |
| top_100_artists_pagerank.csv/ | ~8KB | Top 100 artists by authority |
| cluster_sizes.csv/ | ~200KB | Detailed cluster distribution |
| cluster_bridges.csv/ | ~50KB | Inter-cluster connections |

---

## 🎨 Generated Figures

### Statistical Plots (`figures/report_figures/`)
1. **fig1_degree_distribution.png/pdf** - Power-law degree distribution
2. **fig2_volume_vs_authority.png/pdf** - Top 20 comparison (in-degree vs PageRank)
3. **fig4_cluster_distribution.png/pdf** - Cluster size histogram
4. **fig5_graph_statistics.png/pdf** - Network metrics table
5. **fig6_top_artists_comparison.png/pdf** - Top 10 detailed comparison
6. **fig7_powerlaw_analysis.png/pdf** - Cumulative distribution
7. **fig8_methodology_flowchart.png/pdf** - Project workflow
8. **statistics_summary.txt** - Text statistics

### Network Diagrams (`figures/genealogy_networks/`)
1. **fig1_top15_sampling_network.png/pdf** - Core sampling relationships
2. **fig2_jamesbrown_ego_network.png/pdf** - James Brown influence sphere
3. **fig3_kojikondo_ego_network.png/pdf** - Video game music influence
4. **fig4_sampling_flow.png/pdf** - Bipartite flow diagram
5. **fig5_hub_analysis.png/pdf** - In-degree vs out-degree scatter

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
