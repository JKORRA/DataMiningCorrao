# The Genealogy of Sound: Music Sampling Network Analysis

**Course**: Data Mining  
**Institution**: Università di Trento  
**Student**: Jacopo Corrao  
**Academic Year**: 2025/2026

---

## 📋 Project Overview

This project analyzes the **genealogy of music** through sampling relationships, modeling the music ecosystem as a directed graph where nodes represent songs and edges represent sampling events. Using PageRank (implemented from scratch in Apache Spark) and Louvain community detection (via NetworkX), we identify structural authorities and musical communities beyond traditional popularity metrics. Automated alias resolution merges 161 artist name variants (e.g., "James Brown & The Famous Flames" → "James Brown").

### Key Findings
- **57,741 sampling events** connecting 67,638 songs across 23,242 unique artists
- **Daniel Ingram** emerges as the top structural authority (PageRank: 59.85, Anime/Musical theater composer)
- **James Brown** is the #2 authority (PageRank: 45.67) - the classic "Godfather of Soul"
- **1,774 musical communities** detected (intra-cluster edge fraction: 0.7169)
- **Power-law concentration**: Top 1% of artists control 23.5% of all sampling events
---

## 📁 Project Structure

```
DataMiningCorrao/
├── README.md                          # This file
├── project/                           # Source code and data processing
│   ├── src/                          # Core analysis scripts
│   │   ├── 01_build_graph.py         # Data ingestion and graph construction
│   │   ├── 02_degree_centrality.py   # Degree centrality analysis
│   │   ├── 03_page_rank.py           # PageRank implementation
│   │   ├── 04_louvain_clustering.py  # Louvain community detection
│   │   ├── 05_network_statistics.py  # Network statistics
│   │   ├── 06_cluster_evaluation.py  # Modularity and cluster metrics
│   │   ├── 07_plot_hubs_bridges.py   # Network diagrams
│   │   ├── 08_authority_context.py   # Bridges, authority context, macro flow
│   │   ├── 09_generate_figures.py    # Statistical visualizations
│   │   └── 10_ground_truth_validation.py # Spearman correlation against ground truth
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
│   │   ├── music_clusters.csv        # Community statistics
│   │   ├── music_cluster_membership.csv # Artist-to-community mapping
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
    ├── SpiegazioneFacile.md          # Italian explanation
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
2. Descriptive analysis (volume-based rankings)
3. PageRank authority calculation
4. Louvain community detection
5. Graph validation metrics
6. Cluster quality analysis
7. Cluster-colored artist network (Fig 4)
8. Report-quality hub + bridges visualization (Fig 2)
9. Authority context analysis (Fig 3)
10. Report figure generation (Fig 1 + Fig 5)
11. External validation against ground truth
12. Interactive D3.js visualization

**Option 2: Individual Steps**
```bash
# 1. Prepare data and construct graph
python3 src/01_build_graph.py

# 2. Analyze degree centrality
python3 src/02_degree_centrality.py

# 3. Calculate PageRank authority
python3 src/03_page_rank.py

# 4. Detect communities
python3 src/04_louvain_clustering.py

# 5. Calculate graph validation metrics
python3 src/05_network_statistics.py

# 6. Calculate cluster quality
python3 src/06_cluster_evaluation.py

# 7. Visualize clusters
python3 utils/visualize_cluster.py

# 8. Generate network visualizations
python3 src/07_plot_hubs_bridges.py

# 9. Run advanced experiments
python3 src/08_authority_context.py

# 10. Generate statistical figures
python3 src/09_generate_figures.py

# 11. Run external validation
python3 src/10_ground_truth_validation.py

# 12. Generate interactive visualization
python3 utils/generate_interactive_network.py
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
- `music_clusters.csv` - Community statistics (1,774 clusters)
- `music_cluster_membership.csv` - Artist-to-community mapping
- `music_labels.parquet` - Per-song community assignments
- `interactive_genealogy.html` - **D3.js interactive visualization** (see below)

### Visualizations (`project/figures/report_figures/`)

**Figures**:
1. `fig1_volume_vs_authority.png/pdf` - 3-panel: top 20 by volume, top 20 by authority, and scatter with surprise artists highlighted
2. `fig2_hub_bridges.png/pdf` - Merged hub analysis (in-degree vs out-degree by cluster) and top 15 evolutionary bridges
3. `fig3_authority_context.png/pdf` - Internal vs external influence for top 15 authorities
4. `fig4_cluster_artist_network.png/pdf` - Top 50 artists by PageRank, colored by Louvain cluster (intra/inter edges distinguished)
5. `fig5_cluster_distribution.png/pdf` - Cluster size histogram & top 20 largest communities

**Auxiliary Files**:
- `statistics_summary.txt` - Text statistics summary

---

## 🎨 Interactive Visualization

An interactive D3.js visualization has been generated to explore the music sampling network:

**File**: `project/outputs/interactive_genealogy.html`

**Features**:
- Pan and zoom the entire network with premium SVG glow effects for mega-hubs
- Click on nodes to open a sleek glassmorphism sidebar with artist details and authority scores
- Nodes are colored by their Louvain community (genealogical family)
- Intra-cluster and inter-cluster edges are styled distinctly to reveal community boundaries
- Node size is proportional to PageRank Authority

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
- Runs a fixed 50 iterations with convergence criterion

**Louvain Community Detection**:
- Modularity-maximization algorithm (resolution r=1.0)
- Weighted undirected graph derived from sampling edges
- Detected 1,774 distinct communities
- Largest community: JAY-Z (2,196 artists)
- Intra-cluster edge fraction: 0.7169 (72% of sampling stays within communities)

### 3. Cluster-Colored Artist Network
The `fig4_cluster_artist_network` visualization shows the top 50 artists by PageRank authority, colored by their Louvain cluster. Intra-cluster edges are shown in solid muted colors, while inter-cluster edges appear as gray dashed lines, revealing both community structure and cross-community influence.

---

## 📖 Documentation

### Main Report
- **File**: `report/DataMining.pdf`
- **Language**: English
- **Contents**: Complete methodology, algorithms, results, discussion
- **Figures**: 5 publication-quality visualizations (300 DPI)

### Italian Explanation
- **File**: `report/SpiegazioneFacile.md`
- **Purpose**: Detailed Italian commentary on findings

---

## 🎯 Key Results Summary

### Authority Rankings (PageRank)
1. **Daniel Ingram** - 59.85 (Musical theater / Anime composer)
2. **James Brown** - 45.67 (The Godfather of Soul)
3. **2Pac** - 45.01
4. **電音部 (Den-On-Bu)** - 40.90 (Japanese multimedia project)
5. **外神田文芸高校** - 35.61 (Japanese creative group)

### Network Properties
- **Nodes**: 67,638 songs (23,242 unique artists)
- **Edges**: 57,741 sampling events
- **Graph Type**: Directed, scale-free
- **Density**: 0.00001262 (sparse)
- **Mean In-Degree**: 3.52
- **Mean Out-Degree**: 5.97

### Community Detection
- **Total Communities**: 1,774
- **Intra-Cluster Edge Fraction**: 0.7169
- **Largest Community**: JAY-Z (2,196 artists)
- **Mean Community Size**: 13.07

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

**Last Updated**: 18 Jun. 2026
