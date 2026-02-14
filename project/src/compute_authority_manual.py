import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum, count, when, abs as _abs, max as _max_fn

# 1. Configurazione
spark = SparkSession.builder \
    .appName("MusicGenealogy_PageRank") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoints")

print("Caricamento Grafo...")
df_graph = spark.read.parquet("outputs/music_graph.parquet")

# IMPORTANT: Verify self-loops were already removed during data preparation
# This is a safety check - self-loops should not exist at this stage
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
# CRITICAL FIX: Authority flows FROM the Original (sampled) TO the Sampler
# Interpretation: "Being sampled by influential artists increases YOUR authority"
# links: SRC (Chi è Campionato/Original) -> DST (Chi Campiona/Sampler)
links = df_graph.select(
    col("target_song_id").alias("src"),   # Original Artist (Authority Source)
    col("source_song_id").alias("dst")    # Sampler (Authority Receiver)
).distinct()

# Calcoliamo subito l'Out-Degree (è fisso, non cambia nelle iterazioni)
# Quante canzoni ha campionato X?
out_degrees = links.groupBy("src").agg(count("dst").alias("out_degree"))

# Inizializzazione Ranks
# Troviamo tutti i nodi unici e li chiamiamo subito "id"
nodes = links.select("src").union(links.select("dst")).distinct().select(col("src").alias("id"))

# Rank iniziale = 1.0 per tutti
ranks = nodes.withColumn("rank", lit(1.0))

# =========================================================
# LOOP PAGERANK (10 Iterazioni con Convergenza)
# =========================================================
MAX_ITERATIONS = 20
DAMPING = 0.85
TOLERANCE = 0.0001

print(f"Avvio calcolo PageRank (convergenza < {TOLERANCE})...")

for i in range(MAX_ITERATIONS):
    print(f"--- Iterazione {i+1}/{MAX_ITERATIONS} ---")
    
    # Salva i vecchi ranks per il check di convergenza
    old_ranks = ranks
    
    # 1. Calcoliamo i contributi che partono da SRC
    # Join: Links + Ranks attuali (su SRC = ID) + OutDegree (su SRC)
    # Formula: Contributo = Rank_Attuale / Numero_Link_Uscenti
    contribs = links.join(ranks, links.src == ranks.id) \
                    .join(out_degrees, links.src == out_degrees.src) \
                    .select(
                        col("dst"), 
                        (col("rank") / col("out_degree")).alias("contribution")
                    )
    
    # 2. Sommiamo i contributi che arrivano a DST
    sum_contribs = contribs.groupBy("dst").agg(_sum("contribution").alias("sum_contrib"))
    
    # 3. Calcoliamo il nuovo Rank per chi ha ricevuto voti
    # Formula PageRank: (1-d) + d * somma_voti
    ranks_calculated = sum_contribs.select(
        col("dst").alias("id"),
        (lit(1 - DAMPING) + (lit(DAMPING) * col("sum_contrib"))).alias("rank")
    )
    
    # 4. Gestione Dangling Nodes (Nodi che non sono stati campionati da nessuno in questo giro)
    # Facciamo una Right Join con la lista completa dei nodi per non perdere nessuno
    # Se il rank è null, diamo il punteggio base (1 - d) = 0.15
    ranks = ranks_calculated.join(nodes, "id", "right_outer") \
        .select(
            col("id"),
            when(col("rank").isNull(), lit(1 - DAMPING)).otherwise(col("rank")).alias("rank")
        )
    
    # Checkpoint per troncare la lineage (evita StackOverflowError di Spark)
    ranks = ranks.checkpoint()
    
    # Check convergenza: calcola la differenza massima tra vecchi e nuovi ranks
    diff_check = ranks.join(old_ranks.withColumnRenamed("rank", "old_rank"), "id") \
        .select(_abs(col("rank") - col("old_rank")).alias("abs_diff"))
    
    max_diff = diff_check.agg(_max_fn("abs_diff").alias("max_diff")).collect()[0]["max_diff"]
    
    print(f"    Max rank change: {max_diff:.6f}")
    
    if max_diff < TOLERANCE:
        print(f"✓ Convergenza raggiunta dopo {i+1} iterazioni!")
        break

# =========================================================
# AGGREGAZIONE FINALE
# =========================================================
print("Calcolo completato. Aggregazione per Artista...")

# Mappatura ID Canzone -> Nome Artista
song_artist_map = df_graph.select(
    col("target_song_id").alias("id"), 
    col("Original_Artist_Name").alias("artist")
).distinct()

# Join finale
final_scores = ranks.join(song_artist_map, "id")

# Somma per Artista
artist_authority = final_scores.groupBy("artist") \
    .agg(_sum("rank").alias("authority_score")) \
    .orderBy(col("authority_score").desc())

# =========================================================
# OUTPUT
# =========================================================
print("\n--- CLASSIFICA FINALE: L'AUTORITÀ MUSICALE ---")
artist_authority.show(20, truncate=False)

# Salviamo un CSV piccolino da usare per i grafici
artist_authority.limit(100).write.mode("overwrite").csv("outputs/top_100_artists_pagerank.csv", header=True)

# Salviamo anche il parquet completo per riutilizzo
print("Salvataggio PageRank completo...")
artist_authority.write.mode("overwrite").parquet("outputs/artist_pagerank.parquet")
print("✓ PageRank salvato in: artist_pagerank.parquet e ../outputs/top_100_artists_pagerank.csv")

spark.stop()
