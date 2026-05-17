# The Genealogy of Sound: Music Sampling Network Analysis

**Course**: Data Mining  
**Institution**: Università di Trento  
**Student**: Jacopo Corrao  
**Academic Year**: 2025/2026

---

## 📋 Project Overview

This project analyzes the **genealogy of music** through sampling relationships, modeling the music ecosystem as a directed graph where nodes represent songs and edges represent sampling events. Using distributed graph algorithms (PageRank, Label Propagation) implemented from scratch in Apache Spark, we identify structural authorities and musical communities beyond traditional popularity metrics.

### Key Findings
- **58,621 sampling events** connecting 68,527 songs across 23,503 artists
- **Daniel Ingram** emerges as the top structural authority (PageRank: 59.38, Anime/Musical theater composer)
- **James Brown** is the #2 authority (PageRank: 45.43, 204 samples) - the classic "Godfather of Soul"
- **13,855 musical communities** detected (intra-cluster edge fraction: 0.1926)
- **Power-law concentration**: Top 1% of artists control 23.6% of all sampling events
- Full support for **CJK (Chinese/Japanese/Korean) Unicode characters** in artist names and visualizations

---

## 📁 Project Structure

```
DataMiningCorrao/
├── README.md                          # This file
├── project/                           # Source code and data processing
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
│   ├── mbdump/                       # MusicBrainz database dumps (input)
│   ├── outputs/                      # Analysis results (generated)
│   │   ├── music_graph.parquet       # Processed graph structure
│   │   ├── artist_pagerank.parquet   # PageRank results
│   │   ├── music_clusters.csv        # Cluster assignments
│   │   ├── music_labels.parquet      # Label propagation results
│   │   ├── interactive_genealogy.html # D3.js interactive visualization
│   │   └── cluster_quality_summary.csv
│   │
│   └── figures/                      # Generated visualizations
│       └── report_figures/           # Statistical plots and network diagrams
│
└── report/                           # Final report and documentation
    ├── DataMining.pdf                # Final compiled report (MAIN DELIVERABLE)
    ├── DataMining.tex                # LaTeX source
    ├── SPIEGAZIONE_ITALIANA.md       # Italian explanation
    └── Immagini/                     # Report figures
```

---

## 🚀 Quick Start

### Prerequisites
- **Python 3.8+**
- **Apache Spark 3.5.0** (included in virtual environment)
- **Java 17+** (required by Spark)
- **LaTeX** (for report compilation - optional)

### Installation

1. **Navigate to project directory**:
   ```bash
   cd project/
   ```

2. **Activate virtual environment** (already configured):
   ```bash
   source ../.venv/bin/activate
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
- Data preparation: ~3-5 minutes
- PageRank (convergence): ~4-5 minutes
- Clustering: ~2-3 minutes
- Figure generation: ~1-2 minutes
- **Total pipeline**: ~10-15 minutes

---

## 📊 Output Files

### Analysis Results (`project/outputs/`)
- `music_graph.parquet/` - Cleaned graph (58,621 edges, no self-loops)
- `artist_pagerank.parquet/` - PageRank scores for all artists
- `music_clusters.csv/` - Song-to-cluster assignments
- `music_labels.parquet/` - Label propagation results (13,855 clusters)
- `interactive_genealogy.html` - **D3.js interactive visualization** (see below)

### Visualizations (`project/figures/report_figures/`)

**Figures**:
1. `fig1_top15_sampling_network.png/pdf` - Top 15 artists inter-connection network
2. `fig2_volume_vs_authority.png/pdf` - Top 20 comparison (in-degree vs PageRank)
3. `fig4_cluster_distribution.png/pdf` - Cluster size histogram & top 20
4. `fig6_top_artists_comparison.png/pdf` - Top 10 detailed comparison
5. `fig7_powerlaw_analysis.png/pdf` - Cumulative distribution (log-log)
6. `fig7_hub_analysis.png/pdf` - Hub classification (in-degree vs out-degree)
7. `fig9_top_bridges.png/pdf` - Top 15 evolutionary bridges
8. `fig10_authority_composition.png/pdf` - Internal vs external influence
9. `fig11_macro_community_flow.png/pdf` - Macroscopic inter-community flow

**Auxiliary Files**:
- `table_top10_comparison.tex` - LaTeX table for report
- `statistics_summary.txt` - Text statistics summary

---

## 🎨 Interactive Visualization

An interactive D3.js visualization has been generated to explore the music sampling network:

**File**: `project/outputs/interactive_genealogy.html`

**Features**:
- Pan and zoom the entire network
- Click on nodes to view artist details and authority scores
- Filter and isolate specific sampling lineages
- Interactive legend for node sizing by in-degree

**To view**: Open the file in any modern web browser.

---

## 🔬 Methodology Highlights

### 1. Data Engineering
- **Source**: MusicBrainz PostgreSQL dumps (500K+ relationships)
- **Filtering**: Isolated pure "sampling" relationships (IDs: 69, 231)
- **Self-loop removal**: Filtered out `Sampler_Artist_Name == Original_Artist_Name`
- **Unicode support**: Full CJK (Chinese/Japanese/Korean) character rendering in both Python plots and LaTeX tables
- **Storage**: Optimized Parquet format with columnar compression

### 2. Distributed Graph Algorithms (From Scratch)

**PageRank Implementation**:
- Iterative MapReduce with convergence criterion (tolerance: 0.0001)
- Dangling node handling via right outer join
- Lineage truncation with checkpointing (prevents StackOverflowError)
- Converged in ~11 iterations

**Label Propagation Algorithm (LPA)**:
- Unsupervised community detection (no genre metadata)
- Detected 13,855 distinct clusters
- Identifies musical "genealogical families"

### 3. Top 15 Network Clarity
The `fig1_top15_sampling_network` visualization has been refined to show only **inter-connections** between the top 15 most-sampled artists, removing external clutter nodes for maximum clarity.

---

## 📖 Documentation

### Main Report
- **File**: `report/DataMining.pdf`
- **Language**: English
- **Contents**: Complete methodology, algorithms, results, discussion
- **Figures**: 10 publication-quality visualizations (300 DPI)

### Italian Explanation
- **File**: `report/SPIEGAZIONE_ITALIANA.md`
- **Purpose**: Detailed Italian commentary on findings

---

## 🎯 Key Results Summary

### Authority Rankings (PageRank)
1. **Daniel Ingram** - 59.38 (Musical theater / Anime composer)
2. **James Brown** - 45.43 (The Godfather of Soul)
3. **2Pac** - 44.81
4. **電音部 (Den-On-Bu)** - 38.52 (Japanese multimedia project)
5. **外神田文芸高校** - 33.77 (Japanese creative group)

### Network Properties
- **Nodes**: 68,527 songs (23,503 unique artists)
- **Edges**: 58,621 sampling events
- **Graph Type**: Directed, scale-free
- **Density**: 0.00001248 (sparse)
- **Mean In-Degree**: 3.52
- **Mean Out-Degree**: 6.04

### Community Detection
- **Total Clusters**: 13,855
- **Intra-Cluster Edge Fraction**: 0.1926 (simplified modularity)
- **Largest Cluster**: 675 songs
- **Mean Cluster Size**: 1.69

---

## 🛠️ Technical Stack

- **Apache Spark 3.5.0** (PySpark) - Distributed graph processing
- **NetworkX 3.x** - Network visualization
- **Matplotlib 3.x** - Statistical plotting
- **Pandas 2.x** - Data manipulation
- **Python 3.11+**
- **LaTeX (pdflatex)** - Report compilation
- **D3.js** - Interactive web visualization

---

## 📝 Notes for Reproduction

1. **MusicBrainz Data**: The `mbdump/` directory contains the raw database dumps. These are tab-separated files without headers (schema defined in code).

2. **Checkpointing**: Spark checkpoints are stored in `checkpoints/` and `checkpoints_clustering/` to truncate execution lineage.

3. **Virtual Environment**: A pre-configured `.venv/` is included with all dependencies installed.

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

**Last Updated**: 5 May 2026