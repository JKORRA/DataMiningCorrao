#!/bin/bash

# =============================================================================
# MUSIC GENEALOGY PROJECT - COMPLETE PIPELINE RUNNER
# =============================================================================
# This script runs the entire analysis pipeline in the correct order
# with error checking and progress reporting.
#
# Usage: bash run_pipeline.sh
# =============================================================================

set -e  # Exit on error

# Configure Spark to use local directory instead of /tmp to prevent 'No space left on device' errors
mkdir -p spark_tmp
export SPARK_LOCAL_DIRS="$(pwd)/spark_tmp"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_step() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if we're in the right directory
if [ ! -f "src/data_preparation.py" ]; then
    print_error "Error: src/data_preparation.py not found!"
    print_warning "Please run this script from the dataset directory"
    exit 1
fi

# Check if mbdump directory exists
if [ ! -d "mbdump" ]; then
    print_error "Error: mbdump directory not found!"
    print_warning "Please ensure MusicBrainz data is downloaded and extracted"
    exit 1
fi

# Start timer
SECONDS=0

print_step "MUSIC GENEALOGY PROJECT - COMPLETE PIPELINE"
echo "Starting at: $(date)"

# =============================================================================
# STEP 1: DATA PREPARATION
# =============================================================================
print_step "STEP 1/10: Data Preparation (Building Graph from MusicBrainz)"

if [ -f "outputs/music_graph.parquet/_SUCCESS" ]; then
    print_warning "outputs/music_graph.parquet already exists. Skipping data preparation."
    print_warning "Delete outputs/music_graph.parquet to rebuild from scratch."
else
    python3 src/data_preparation.py
    if [ $? -eq 0 ]; then
        print_success "Graph built successfully → outputs/music_graph.parquet"
    else
        print_error "Data preparation failed!"
        exit 1
    fi
fi

# =============================================================================
# STEP 2: DESCRIPTIVE ANALYSIS
# =============================================================================
print_step "STEP 2/10: Descriptive Analysis (Volume-based Rankings)"

python3 src/top_ranking.py
if [ $? -eq 0 ]; then
    print_success "Top rankings calculated"
else
    print_error "Top ranking analysis failed!"
    exit 1
fi

# =============================================================================
# STEP 3: PAGERANK AUTHORITY
# =============================================================================
print_step "STEP 3/10: PageRank Authority Calculation (WITH FIXES)"

python3 src/compute_authority_manual.py
if [ $? -eq 0 ]; then
    print_success "PageRank completed → outputs/artist_pagerank.parquet"
else
    print_error "PageRank calculation failed!"
    exit 1
fi

# =============================================================================
# STEP 4: CLUSTERING
# =============================================================================
print_step "STEP 4/10: Label Propagation Clustering"

python3 src/cluster.py
if [ $? -eq 0 ]; then
    print_success "Clustering completed → outputs/music_labels.parquet"
else
    print_error "Clustering failed!"
    exit 1
fi

# =============================================================================
# STEP 5: VALIDATION METRICS
# =============================================================================
print_step "STEP 5/10: Graph Validation Metrics (NEW)"

python3 src/validation_metrics.py
if [ $? -eq 0 ]; then
    print_success "Validation metrics calculated → outputs/validation_summary.csv"
else
    print_error "Validation metrics failed!"
    exit 1
fi

# =============================================================================
# STEP 6: CLUSTER QUALITY
# =============================================================================
print_step "STEP 6/10: Cluster Quality Analysis (NEW)"

python3 src/cluster_quality.py
if [ $? -eq 0 ]; then
    print_success "Cluster quality evaluated → outputs/cluster_quality_summary.csv"
else
    print_error "Cluster quality analysis failed!"
    exit 1
fi

# =============================================================================
# STEP 7: VISUALIZATION
# =============================================================================
print_step "STEP 7/10: Network Visualization (IMPROVED)"

python3 utils/visualize_cluster.py
if [ $? -eq 0 ]; then
    print_success "Visualization generated → music_genealogy_final.png"
else
    print_error "Visualization failed!"
    exit 1
fi

# =============================================================================
# STEP 8: REPORT FIGURES
# =============================================================================
print_step "STEP 8/10: Generate Report Figures (Publication Quality)"

python3 src/generate_report_figures.py
if [ $? -eq 0 ]; then
    print_success "Report figures generated → figures/report_figures/"
else
    print_error "Report figure generation failed!"
    exit 1
fi

# =============================================================================
# STEP 9: EXTERNAL VALIDATION
# =============================================================================
print_step "STEP 9/10: External Validation against Ground Truth"

python3 src/external_validation.py
if [ $? -eq 0 ]; then
    print_success "Validation generated → outputs/external_validation.csv"
else
    print_error "External validation failed!"
    exit 1
fi

# =============================================================================
# STEP 10: INTERACTIVE VISUALIZATION
# =============================================================================
print_step "STEP 10/10: Generate Interactive Network (D3.js)"

python3 utils/generate_interactive_network.py
if [ $? -eq 0 ]; then
    print_success "Interactive viz generated → outputs/interactive_genealogy.html"
else
    print_error "Interactive visualization failed!"
    exit 1
fi

# =============================================================================
# SUMMARY
# =============================================================================
duration=$SECONDS

print_step "PIPELINE COMPLETE"

echo -e "${GREEN}All analyses completed successfully!${NC}\n"

echo "📊 Generated Files (outputs/):"
echo "  • music_graph.parquet - Main graph structure"
echo "  • artist_pagerank.parquet - Authority scores"
echo "  • music_labels.parquet - Cluster assignments"
echo "  • top_100_artists_pagerank.csv - Top artists by authority"
echo "  • music_clusters.csv - Cluster sizes"
echo "  • validation_summary.csv - Graph statistics"
echo "  • cluster_quality_summary.csv - Clustering metrics"
echo "  • cluster_sizes.csv - Detailed cluster distribution"
echo "  • cluster_bridges.csv - Inter-cluster connections"
echo "  • external_validation.csv - Ground truth correlation"
echo "  • interactive_genealogy.html - D3.js Network Explorer"
echo ""
echo "📈 Report Figures (figures/):"
echo "  report_figures/ - 10 statistical/network figures (PNG + PDF)"
echo "  • Publication-quality figures (300 DPI)"

echo ""
echo "⏱️  Total execution time: $(($duration / 60)) minutes $(($duration % 60)) seconds"
echo "📅 Completed at: $(date)"

echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "  1. Review outputs/validation_summary.csv for graph statistics"
echo "  2. Check outputs/cluster_quality_summary.csv for modularity score"
echo "  3. View figures/report_figures/ for network visualizations"
echo "  4. Read ../README.md for complete project documentation"

print_success "End!"
