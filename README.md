# The Genealogy of Sound: Music Sampling Network Analysis

**Course**: Data Mining  
**Institution**: Università di Trento  
**Student**: Jacopo Corrao  
**Academic Year**: 2025/2026

---

## 📋 Project Overview

This project analyzes the **genealogy of music** through sampling relationships, modeling the music ecosystem as a directed graph where nodes represent songs and edges represent sampling events. Using distributed graph algorithms (PageRank, Label Propagation) implemented from scratch in Apache Spark, we identify structural authorities and musical communities beyond traditional popularity metrics.

### Key Findings
- **20,728 sampling events** connecting 30,021 songs across 13,587 artists
- **James Brown** emerges as the structural "Godfather" (PageRank: 11.70, 183 samples)
- **Koji Kondo** (Nintendo composer) reveals cross-genre influence: video games → hip hop
- **7,954 musical communities** detected (modularity: 0.2724)
- **Power-law concentration**: Top 1% of artists control 21.8% of all sampling

---

## 📁 Project Structure

```
MusicProject/
├── README.md                          # This file
├── dataset/                           # Source code and data processing
│   ├── src/                          # Core analysis scripts
│   │   ├── data_preparation.py       # Data ingestion and graph construction
│   │   ├── compute_authority_manual.py # PageRank implementation
│   │   ├── cluster.py                # Label Propagation clustering
│   │   ├── cluster_quality.py        # Modularity and cluster metrics
│   │   ├── top_ranking.py            # Degree centrality analysis
│   │   ├── validation_metrics.py     # Network statistics
│   │   ├── generate_report_figures.py # Statistical visualizations
│   │   └── genealogy_visualizations.py # Network diagrams
│   │
│   ├── utils/                        # Utility scripts
│   │   ├── analyze_selfloops.py      # Self-loop detection analysis
│   │   └── visualize_cluster.py      # Individual cluster visualization
│   │
│   ├── run_pipeline.sh               # Complete analysis pipeline
│   ├── requirements.txt              # Python dependencies
│   │
│   ├── mbdump/                       # MusicBrainz database dumps (input)
│   ├── outputs/                      # Analysis results (generated)
│   │   ├── music_graph.parquet       # Processed graph structure
│   │   ├── artist_pagerank.parquet   # PageRank results
│   │   ├── music_clusters.csv        # Cluster assignments
│   │   ├── cluster_quality_summary.csv
│   │   ├── validation_summary.csv
│   │   └── top_100_artists_pagerank.csv
│   │
│   └── figures/                      # Generated visualizations
│       ├── report_figures/           # Statistical plots (8 figures)
│       └── genealogy_networks/       # Network diagrams (5 figures)
│
└── report/                           # Final report and documentation
    ├── DataMining.pdf                # Final compiled report (MAIN DELIVERABLE)
    ├── DataMining.tex                # LaTeX source
    ├── SPIEGAZIONE_ITALIANA.md       # Italian explanation
    └── Immagini/                     # Report figures (12 images)
```

---

## 🚀 Quick Start

### Prerequisites
- **Python 3.8+**
- **Apache Spark 3.5.0** (included in virtual environment)
- **Java 17+** (required by Spark)
- **LaTeX** (for report compilation - optional)

### Installation

1. **Navigate to dataset directory**:
   ```bash
   cd dataset/
   ```

2. **Activate virtual environment** (already configured):
   ```bash
   source venv/bin/activate
   ```

3. **Verify dependencies** (already installed):
   ```bash
   pip list | grep -E "pyspark|networkx|matplotlib|pandas"
   ```

### Running the Analysis

**Option 1: Complete Pipeline** (recommended)
```bash
./run_pipeline.sh
```
This executes the full analysis pipeline:
1. Data preparation and graph construction
2. PageRank authority calculation
3. Label Propagation clustering
4. Cluster quality metrics
5. Network validation
6. Figure generation

**Option 2: Individual Steps**
```bash
# 1. Prepare data and construct graph
python3 src/data_preparation.py

# 2. Calculate PageRank authority
python3 src/compute_authority_manual.py

# 3. Detect communities
python3 src/cluster.py

# 4. Calculate cluster quality
python3 src/cluster_quality.py

# 5. Analyze degree centrality
python3 src/top_ranking.py

# 6. Generate statistical figures
python3 src/generate_report_figures.py

# 7. Generate network visualizations
python3 src/genealogy_visualizations.py
```

### Expected Runtime
- Data preparation: ~2-3 minutes
- PageRank (11 iterations): ~3-4 minutes
- Clustering: ~2 minutes
- Figure generation: ~1 minute
- **Total pipeline**: ~8-10 minutes

---

## 📊 Output Files

### Analysis Results (`dataset/outputs/`)
- `music_graph.parquet/` - Cleaned graph (20,728 edges, no self-loops)
- `artist_pagerank.parquet/` - PageRank scores for all artists
- `music_clusters.csv/` - Song-to-cluster assignments (7,954 clusters)
- `validation_summary.csv/` - Network statistics
- `cluster_quality_summary.csv/` - Modularity, sizes, bridge analysis

### Visualizations (`dataset/figures/`)

**Statistical Figures** (8 files):
1. `fig1_degree_distribution.png` - Power-law degree distribution
2. `fig2_volume_vs_authority.png` - Top 20 comparison (in-degree vs PageRank)
3. `fig4_cluster_distribution.png` - Cluster size histogram
4. `fig5_graph_statistics.png` - Network metrics summary
5. `fig6_top_artists_comparison.png` - Top 10 detailed table
6. `fig7_powerlaw_analysis.png` - Cumulative distribution
7. `fig8_methodology_flowchart.png` - Project workflow
8. `statistics_summary.txt` - Text statistics

**Network Diagrams** (5 files):
1. `fig1_top15_sampling_network.png` - Core sampling relationships
2. `fig2_jamesbrown_ego_network.png` - James Brown's influence sphere
3. `fig3_kojikondo_ego_network.png` - Video game music influence
4. `fig4_sampling_flow.png` - Bipartite flow visualization
5. `fig5_hub_analysis.png` - In-degree vs out-degree classification

---

## 🔬 Methodology Highlights

### 1. Data Engineering
- **Source**: MusicBrainz PostgreSQL dumps (500K+ relationships)
- **Filtering**: Isolated pure "sampling" relationships (IDs: 69, 231)
- **Critical improvement**: Removed 1,407 self-loops (6.4% of edges) to prevent authority inflation
- **Storage**: Optimized Parquet format with columnar compression

### 2. Distributed Graph Algorithms (From Scratch)

**PageRank Implementation**:
- Iterative MapReduce with convergence criterion (tolerance: 0.0001)
- Dangling node handling via right outer join
- Lineage truncation with checkpointing (prevents StackOverflowError)
- Converged in 11 iterations

**Label Propagation Algorithm**:
- 6 iterations to convergence
- Unsupervised community detection (no genre metadata)
- Discovered 7,954 musical families

### 3. Self-Loop Removal Rationale
Self-loops (artist sampling themselves) were identified as a critical data quality issue:
- **Problem**: Creates artificial authority inflation (e.g., "Ninja McTits": 443 self-loops → PageRank 66.45)
- **Solution**: Filter `Sampler_Artist_Name != Original_Artist_Name`
- **Impact**: Ensures only cross-artist influence is measured

---

## 📖 Documentation

### Main Report
- **File**: `report/DataMining.pdf`
- **Language**: English
- **Contents**: Complete methodology, algorithms, results, discussion
- **Figures**: 12 publication-quality visualizations (300 DPI)

### Italian Explanation
- **File**: `report/SPIEGAZIONE_ITALIANA.md`
- **Purpose**: Detailed Italian commentary on findings
- **Sections**: Network statistics, authority rankings, genealogy analysis

---

## 🎯 Key Results Summary

### Authority Rankings (PageRank)
1. **James Brown** - 11.70 (The Godfather)
2. **Beastie Boys** - 9.19
3. **Daft Punk** - 7.53
4. **Jay-Z** - 7.06
5. **The Notorious B.I.G.** - 6.60

### Network Properties
- **Nodes**: 30,021 songs (13,587 unique artists)
- **Edges**: 20,728 sampling events (after self-loop removal)
- **Graph Type**: Directed, scale-free
- **Density**: 0.00002456 (sparse)
- **Mean In-Degree**: 2.57
- **Mean Out-Degree**: 3.23

### Community Detection
- **Total Clusters**: 7,954
- **Modularity**: 0.2724
- **Largest Cluster**: 143 songs
- **Mean Cluster Size**: 2.49

---

## 🛠️ Technical Stack

- **Apache Spark 3.5.0** (PySpark) - Distributed graph processing
- **NetworkX 3.1** - Network visualization
- **Matplotlib 3.7.1** - Statistical plotting
- **Pandas 2.0.2** - Data manipulation
- **Python 3.11**
- **LaTeX (pdflatex)** - Report compilation

---

## 📝 Notes for Reproduction

1. **MusicBrainz Data**: The `mbdump/` directory contains the raw database dumps. These are tab-separated files without headers (schema defined in code).

2. **Checkpointing**: Spark checkpoints are stored in `checkpoints/` and `checkpoints_clustering/` to truncate execution lineage.

3. **Virtual Environment**: A pre-configured `venv/` is included with all dependencies installed.

4. **Memory Requirements**: Recommended 8GB+ RAM for Spark driver/executors.

5. **Reproducibility**: All random seeds are fixed for deterministic clustering results.

---

## 📧 Contact

**Student**: Jacopo Corrao  
**Course**: Data Mining  
**University**: Università di Trento  

---

## 📄 License & Data Attribution

- **MusicBrainz Data**: CC BY-NC-SA 3.0 (Non-commercial use)
- **Code**: Educational project for academic evaluation
- **Report**: © 2026 Jacopo Corrao

---

**Last Updated**: 12 February 2026
