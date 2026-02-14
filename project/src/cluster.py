import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as _max, count, first, lit, greatest

# 1. Configurazione
spark = SparkSession.builder \
    .appName("MusicGenealogy_Clustering") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoints_clustering")

print("Caricamento Grafo per Clustering...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")

# IMPORTANT: Verify self-loops were already removed
self_loops = df_graph.filter(col("Sampler_Artist_Name") == col("Original_Artist_Name")).count()
if self_loops > 0:
    print(f"WARNING: Found {self_loops} self-loops! Removing them now...")
    df_graph = df_graph.filter(col("Sampler_Artist_Name") != col("Original_Artist_Name"))
else:
    print("✓ No self-loops detected (good!)")

print(f"Graph loaded: {df_graph.count()} edges")

# =========================================================
# PREPARAZIONE
# =========================================================
# DESIGN CHOICE: Edges represent sampling relationships
# - child = source_song_id (The artist who SAMPLES)
# - parent = target_song_id (The artist being SAMPLED)
# 
# CLUSTERING INTERPRETATION:
# Artists who sample the SAME sources form a "genealogical family"
# This creates clusters based on shared influences, not acoustic similarity
edges = df_graph.select(
    col("source_song_id").alias("child"), 
    col("target_song_id").alias("parent")
).distinct()

# Lista di tutti i nodi
nodes = edges.select("child").union(edges.select("parent")).distinct()

# INIZIALIZZAZIONE:
labels = nodes.select(col("child").alias("id"), col("child").alias("label"))

# =========================================================
# CICLO DI CLUSTERING (Label Propagation)
# =========================================================
# ALGORITHM: Label Propagation for Community Detection
# - Each node adopts the label of its most influential neighbor
# - Iterative process creates "genealogical families" based on shared influences
# - Unsupervised: No predefined number of clusters
ITERATIONS = 6 
print(f"Avvio Label Propagation per {ITERATIONS} iterazioni...")

for i in range(ITERATIONS):
    print(f"--- Iterazione {i+1}/{ITERATIONS} ---")
    
    # 1. Join: Ogni Figlio guarda l'etichetta del Padre
    propagation = edges.join(labels, edges.parent == labels.id) \
                       .select(col("child"), col("label").alias("parent_label"))
    
    # 2. Aggregazione: Trova l'etichetta più grande proposta dai padri
    current_state = labels.alias("l").join(propagation.alias("p"), col("l.id") == col("p.child"), "left_outer") \
        .select(
            col("l.id"),
            col("l.label").alias("old_label"),
            col("p.parent_label").alias("new_proposal")
        )
    
    # CORREZIONE QUI:
    # Usiamo _max per aggregare le proposte (verticale)
    # Usiamo greatest per scegliere tra vecchia e nuova (orizzontale)
    labels = current_state.groupBy("id") \
        .agg(_max(col("new_proposal")).alias("max_proposal"), first("old_label").alias("old")) \
        .select(
            col("id"),
            # greatest() confronta due colonne e prende la maggiore (ignorando i null)
            greatest(col("max_proposal"), col("old")).alias("label") 
        ).checkpoint()

# =========================================================
# ANALISI DEI CLUSTER TROVATI
# =========================================================
print("Clustering completato. Aggregazione risultati...")

# Mappatura ID -> Nome Artista
song_artist_map = df_graph.select(
    col("target_song_id").alias("song_id"), 
    col("Original_Artist_Name").alias("artist_name")
).distinct()

# Join finale
final_clusters = labels.join(song_artist_map, labels.label == song_artist_map.song_id) \
    .select(
        col("id").alias("song_id"),
        col("label").alias("cluster_id"),
        col("artist_name").alias("cluster_representative")
    )

# Conteggio dimensioni cluster
cluster_sizes = final_clusters.groupBy("cluster_representative") \
    .agg(count("*").alias("cluster_size")) \
    .orderBy(col("cluster_size").desc())

print("\n--- I DOMINATORI DELLE COMMUNITY (CLUSTERS) ---")
print("Questi sono gli artisti che definiscono le 'Famiglie' musicali più grandi:")
cluster_sizes.show(20, truncate=False)

# Salvataggio
print("Salvataggio risultati...")
final_clusters.write.mode("overwrite").parquet("outputs/music_labels.parquet")
cluster_sizes.limit(100).write.mode("overwrite").csv("outputs/music_clusters.csv")
print("✓ Clusters salvati in: music_labels.parquet e ../outputs/music_clusters.csv")

spark.stop()
