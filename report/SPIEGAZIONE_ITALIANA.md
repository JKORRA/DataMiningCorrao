# Spiegazione Dettagliata del Report - Italiano

## Titolo del Progetto
**La Genealogia del Suono: Analisi Distribuita delle Reti di Influenza Musicale tramite Mining del Grafo di Sampling**

---

## Indice della Spiegazione

1. [Introduzione e Motivazione](#1-introduzione-e-motivazione)
2. [Preparazione dei Dati](#2-preparazione-dei-dati)
3. [Metodologia e Algoritmi](#3-metodologia-e-algoritmi)
4. [Risultati](#4-risultati)
5. [Discussione e Interpretazione](#5-discussione-e-interpretazione)
6. [Conclusione](#6-conclusione)

---

## 1. Introduzione e Motivazione

### Problema Principale
L'industria musicale misura tradizionalmente il successo attraverso le classifiche di vendita e i numeri di streaming (popolarità). Tuttavia, l'**impatto culturale** è strutturalmente diverso dalla popolarità.

**Esempio concreto:**
- Una "one-hit wonder" potrebbe vendere milioni di copie
- MA una traccia funk oscura degli anni '70 potrebbe essere campionata da centinaia di artisti hip-hop, formando la spina dorsale di un intero genere

### Obiettivo del Progetto
Spostare l'analisi dal **volume** alla **struttura**. 

**Come?** Modellando l'ecosistema musicale come un **Grafo Diretto** dove:
- **Nodi** = Canzoni
- **Archi** = Relazioni di sampling (chi ha campionato chi)

### Domande di Ricerca
1. Chi sono gli "antenati" strutturali della musica moderna? (Autorità)
2. Esistono comunità di influenza che trascendono le etichette di genere tradizionali?
3. Come possiamo analizzare il flusso di influenza usando algoritmi distribuiti su grafo?

---

## 2. Preparazione dei Dati

### 2.1 Sorgente del Dataset: MusicBrainz

**Cos'è MusicBrainz?**
- Un'enciclopedia musicale aperta che raccoglie metadati contribuiti dagli utenti
- Fornisce dump del database PostgreSQL completo (NON dataset semplificati tipo Kaggle)

**Perché è complesso?**
- Struttura **altamente normalizzata** (Terza Forma Normale)
- Richiede significativo lavoro di data engineering per ricostruire relazioni significative

**Tabelle utilizzate:**
- `recording` - canzoni
- `artist_credit` - nomi degli artisti
- `l_recording_recording` - relazioni tra canzoni
- `link` - tabella ponte che connette le relazioni
- `link_type` - definizioni semantiche delle relazioni

### 2.2 Acquisizione ed Engineering dei Dati

**Formato grezzo:**
- File TSV (Tab-Separated Values) **senza header**
- Estratti dall'archivio `mbdump.tar.bz2`

**Processo di trasformazione in 3 fasi:**

#### Fase 1: Definizione dello Schema
**Problema:** File senza header, schema inference automatica costosa e soggetta a errori

**Soluzione:** Schema manuale definito usando `StructType` di PySpark

**Esempio di codice:**
```python
recording_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("gid", StringType(), True),
    StructField("name", StringType(), True),
    StructField("artist_credit", IntegerType(), True),
    # ... altri campi
])
```

**Vantaggi:**
- Tipizzazione rigorosa
- Ingestione dati robusta
- Nessun spreco computazionale

#### Fase 2: Proiezione e Filtraggio
**Column Pruning (Potatura delle Colonne):**
- Conservo SOLO le colonne essenziali (ID e nomi)
- Scarto metadati inutili (`edits_pending`, `comments`)
- Ottimizzazione della memoria su driver ed executor Spark

**Filtraggio Critico - Solo Sampling:**

**Problema:** La tabella `l_recording_recording` contiene MILIONI di relazioni generiche:
- Cover
- Remix
- Medley
- Performance dal vivo
- **Sampling** ← QUESTO CI INTERESSA!

**Soluzione:** Query esplorativa sulla tabella `link_type` per identificare gli ID specifici del sampling:

**ID identificati:**
- **ID 69**: "Samples material" (tipo legacy)
- **ID 231**: "Is sampled by" / "Samples material" (tipo corrente)

**Risultato:** Solo gli archi che corrispondono a questi ID vengono mantenuti → rete pura di trasmissione audio

#### Fase 3: Ricostruzione del Grafo
**Problema:** Il database è normalizzato. Una relazione di sampling è memorizzata come:
```
entity0 (ID numerico) → entity1 (ID numerico)
```
**Senza** informazioni testuali su titolo canzone o nome artista.

**Soluzione:** Pipeline di de-normalizzazione con catena di join su 4 DataFrame:

1. **Linkage:** Join `l_recording_recording` + `link` 
   - Accesso all'attributo `link_type`
   - Filtraggio per ID 69, 231

2. **Arricchimento Source:** Join con `recording` + `artist_credit`
   - Risolve nome canzone Source (Child)
   - Risolve nome artista Source

3. **Arricchimento Target:** Join con `recording` + `artist_credit`
   - Risolve nome canzone Target (Parent)
   - Risolve nome artista Target

**Risultato:** Grafo leggibile da umani:
```
"Artist A samples Artist B"
```

### 2.3 Scelta del Framework Computazionale: Apache Spark

**Perché NON Pandas?**
- Complessità relazionale del database MusicBrainz (altamente normalizzato)
- Dimensione potenziale del grafo
- Join multi-way e calcoli iterativi inefficienti su singolo nodo

**Perché Apache Spark (PySpark)?**

1. **Processing Distribuito**
   - Ingestione e trasformazione dati in parallelo

2. **Lazy Evaluation**
   - Ottimizzazione del piano di esecuzione di pipeline di trasformazione complesse

3. **Calcolo Iterativo**
   - Essenziale per implementare algoritmi su grafo come PageRank da zero

### 2.4 Ottimizzazione: Formato Parquet

**Problema:** Il costo computazionale di costruire il grafo (join multipli su milioni di righe) è significativo.

**Soluzione:** Persistenza del grafo finale su disco in formato **Apache Parquet**

**Vantaggi rispetto a CSV:**

1. **Storage Colonnare**
   - Spark può leggere SOLO le colonne necessarie per una query specifica
   - Esempio: Recuperare solo `source_id` e `target_id` per analisi topologica
   - Riduzione drastica dell'I/O overhead

2. **Preservazione dello Schema**
   - Mantiene metadati e tipi di dati (Integer, String)
   - Elimina necessità di schema inference o casting durante la lettura

3. **Compressione**
   - Algoritmi di compressione efficienti
   - Riduzione del footprint su disco del dataset finale

### 2.5 Rimozione dei Self-Loop (Miglioramento Critico)

**Problema identificato:** Durante l'analisi esplorativa, abbiamo scoperto **1,407 self-loop** (archi dove un artista campiona se stesso), pari al 6.4% di tutti gli archi.

**Cosa sono i self-loop?**

I self-loop si verificano quando `Sampler_Artist_Name == Original_Artist_Name`, e tipicamente rappresentano:

1. **Remix** - Un artista che remixa la propria traccia
2. **Ri-edizioni** - Versioni diverse della stessa canzone (album vs. singolo)
3. **Struttura degli album** - Intro/outro che campionano altre tracce dello stesso album
4. **Artefatti di dati** - Errori nel database o voci duplicate

**Perché sono problematici?**

#### 1. Inflazione Artificiale dell'Autorità (PageRank)
I self-loop creano feedback loop dove i nodi aumentano artificialmente il proprio punteggio.

**Esempio concreto:**
- **Ninja McTits**: 443 self-loop → PageRank 66.45 (completamente artificiale!)
- **James Brown** (legittimo #1): 0 self-loop → PageRank 11.70

#### 2. Genealogia Fuorviante
Il self-sampling NON rappresenta influenza tra artisti diversi. È "riciclaggio interno" artistico, non genealogia musicale.

**Concetto chiave:** Vogliamo tracciare idee che **fluiscono tra artisti diversi**, non versioni alternative della stessa canzone.

#### 3. Problemi di Visualizzazione
- Nodi isolati (connessi solo a se stessi)
- Struttura di rete confusa
- Nella Figura 1 del Top 15, solo 8 artisti erano visibili e 3 erano isolati

**Soluzione implementata:**

Seguendo le best practice dell'analisi delle reti di citazione, tutti i self-loop sono stati rimossi durante la fase di preparazione dati:

```python
# Filtro applicato dopo la costruzione del grafo
edges_before = df_final_graph.count()
df_final_graph = df_final_graph.filter(
    col("Sampler_Artist_Name") != col("Original_Artist_Name")
)
edges_after = df_final_graph.count()
print(f"Self-loop rimossi: {edges_before - edges_after} archi")
```

**Risultato:**
- Archi prima: 22,135
- Archi dopo: 20,728
- Self-loop rimossi: 1,407 (6.4%)

**Impatto:**
- ✅ Punteggi PageRank puliti (Ninja McTits rimosso dal ranking)
- ✅ Visualizzazioni di rete chiare (nessun nodo isolato)
- ✅ Analisi che misura solo influenza **esterna** (obiettivo del progetto)

**Giustificazione accademica:**

Questa è **pratica standard** nella scienza delle reti:
- **Google PageRank**: Rimuove self-citazioni (paper che citano se stessi)
- **Reti Sociali**: Studi di propagazione dell'influenza escludono self-loop
- **Teoria dei Grafi**: Self-loop generalmente esclusi per misure di centralità

---

## 3. Metodologia e Algoritmi

Con la struttura del grafo ottimizzata e persistita, l'analisi si è concentrata sull'estrarre conoscenza attraverso tre livelli distinti di complessità.

### 3.1 Analisi Descrittiva della Rete: Centralità di Grado

**Primo livello:** Quantificare il "volume" usando metriche base di topologia del grafo.

**Metriche calcolate usando aggregazioni Spark SQL:**

#### In-Degree (Più Campionato)
**Cosa misura:** Numero di archi entranti

**Significato:** Quantifica la popolarità grezza di un artista come materiale sorgente

**Implementazione:**
```python
df_graph.groupBy("Original_Artist_Name").count()
```

**Interpretazione:**
- In-Degree alto = Artista molto campionato
- Esempio: James Brown con 183 in-degree (dopo rimozione self-loop)

#### Out-Degree (Top Campionatori)
**Cosa misura:** Numero di archi uscenti

**Significato:** Identifica artisti che si affidano maggiormente al sampling per il loro processo compositivo

**Implementazione:**
```python
df_graph.groupBy("Sampler_Artist_Name").count()
```

**Interpretazione:**
- Out-Degree alto = Artista che usa molti campioni
- Tipico dei produttori hip-hop moderni

### 3.2 Autorità Strutturale: PageRank Distribuito

**Problema:** In-Degree misura la popolarità (quantità), ma **non cattura la qualità** dell'influenza.

**Soluzione:** Modellare l'"Autorità" di un artista usando l'**algoritmo PageRank**

#### Principio Fondamentale di PageRank
> "Essere campionato da un artista influente trasmette più autorità che essere campionato da uno minore"

**Analogia:**
- In-Degree = Contare quante persone ti citano
- PageRank = Pesare le citazioni in base all'importanza di chi ti cita

#### Formula Standard
$$PR(A) = (1 - d) + d \sum_{B \to A} \frac{PR(B)}{OutDegree(B)}$$

Dove:
- $d$ = damping factor (0.85)
- $B \to A$ = archi che puntano ad A
- $OutDegree(B)$ = numero di archi uscenti da B

#### Implementazione PySpark: Processo Iterativo MapReduce

**Caratteristiche:**
- Implementato **manualmente** (NON usando librerie pre-costruite come GraphFrames)
- Processo iterativo con **criterio di convergenza**
- **11 iterazioni** per convergere (tolleranza < 0.0001)

**Direzione degli Archi:**
Gli archi puntano da Sampler a Original Artist, così che l'Original Artist (essendo campionato) riceve autorità attraverso archi **entranti**.

#### Sfide Tecniche Affrontate

**1. Nodi Pendenti (Sink)**
**Problema:** Nodi senza archi uscenti (artisti campionati ma che non campionano nessuno)

**Conseguenza:** Agiscono come "buchi neri" per il punteggio di rank

**Soluzione:** Uso di `right_outer_join` per identificare questi nodi e gestire la ridistribuzione del punteggio

**2. Troncamento della Lineage**
**Problema:** La lazy evaluation di Spark costruisce un grafo di lineage crescente ad ogni iterazione

**Conseguenza:** Può portare a `StackOverflowError`

**Soluzione:** Applicazione di `.checkpoint()` ad ogni loop
- Tronca il piano logico
- Salva lo stato intermedio su disco

### 3.3 Rilevamento di Comunità: Label Propagation Algorithm (LPA)

**Obiettivo:** Identificare distinte "famiglie musicali"

**Algoritmo:** Label Propagation Algorithm (LPA)
- Tecnica di clustering **non supervisionato**
- I nodi adottano l'"etichetta" dei loro vicini basandosi su voto di maggioranza

#### Logica dell'Algoritmo

**Inizializzazione:**
- Ogni artista inizia con un'etichetta unica uguale al proprio ID

**Propagazione (6 Iterazioni):**
- In ogni iterazione, un nodo "Sampler" adotta l'etichetta del nodo "Sampled"
- Se un nodo campiona più artisti, la funzione `greatest()` seleziona deterministicamente l'etichetta dominante

**Convergenza:**
- Nel corso di più iterazioni, componenti connesse (catene di sampling) convergono verso un'unica etichetta condivisa
- L'etichetta finale = ID dell'"Antenato" o artista "Radice"

#### Definizione Strutturale di Generi (Apprendimento Non Supervisionato)

**Distinzione cruciale:** L'algoritmo opera **alla cieca** rispetto ai generi musicali espliciti.

**NON abbiamo importato:**
- Tabelle di metadati contenenti tag come "Hip Hop" o "Rock"

**L'algoritmo si basa interamente su pattern strutturali:**
> Se un insieme di artisti campiona sistematicamente lo stesso materiale sorgente, vengono raggruppati nello stesso cluster

**Validazione dell'Ipotesi:**
I generi musicali sono fondamentalmente **comunità di pratica condivisa**.

---

## 4. Risultati

### 4.1 Statistiche della Rete e Validazione

**Grafo finale processato (dopo rimozione self-loop):**
- **20,728** eventi di sampling (archi) - dopo aver rimosso 1,407 self-loop
- **30,021** canzoni (nodi)
- **13,587** artisti unici

**Nota importante:** La rimozione dei self-loop (6.4% degli archi grezzi) è stata essenziale per garantire che la rete rappresenti genuina influenza **cross-artista** piuttosto che riciclaggio artistico interno.

#### Caratteristiche di Rete Scale-Free

La rete esibisce caratteristiche classiche **scale-free**, evidenziate dalla distribuzione di grado power-law.

**Proprietà Chiave della Rete:**

| Metrica | Valore | Significato |
|---------|--------|-------------|
| **Densità del Grafo** | 0.00002456 | Rete sparsa (tipica delle reti di influenza reali) |
| **In-Degree Medio** | 2.57 | Volte medie che un artista viene campionato |
| **Out-Degree Medio** | 3.23 | Campioni medi usati per artista |
| **In-Degree Massimo** | 183 | James Brown (artista più campionato) |
| **Out-Degree Massimo** | 439 | Girl Talk (artista che più campiona) |

#### Concentrazione Power-Law

**Pattern di Concentrazione:**
- Top 1% degli artisti controlla il **21.8%** di tutti gli eventi di sampling
- Top 10% controlla la maggioranza significativa di tutti gli eventi di sampling

**Validazione:** Questo pattern conferma l'ipotesi di rete scale-free.

**Spiegazione:**
L'influenza musicale segue **preferential attachment**:
> Le autorità consolidate accumulano influenza in modo sproporzionato nel tempo

**Analogia:** I ricchi diventano più ricchi

### 4.2 Classifiche di Autorità PageRank

**Convergenza:** L'algoritmo è converguto in **11 iterazioni** (tolleranza < 0.0001)

#### Top 10 Artisti per Punteggio di Autorità

| Rank | Artista | PageRank | In-Degree |
|------|---------|----------|-----------|
| 1 | James Brown | 11.70 | 183 |
| 2 | Beastie Boys | 9.19 | 67 |
| 3 | Daft Punk | 7.53 | 99 |
| 4 | Jay-Z | 7.06 | 44 |
| 5 | The Notorious B.I.G. | 6.60 | 34 |
| 6 | The Beatles | 6.53 | 77 |
| 7 | The Rolling Stones | 6.15 | 44 |
| 8 | 近藤浩治 (Koji Kondo) | 6.15 | 49 |
| 9 | [unknown] | 5.87 | 85 |
| 10 | OutKast | 5.80 | 37 |

**Nota:** Dopo la rimozione dei self-loop, i valori riflettono solo influenza cross-artista. Jay-Z e The Notorious B.I.G. dimostrano che la posizione strutturale conta quanto il volume grezzo (PageRank alto con in-degree relativamente basso).

### 4.3 Volume vs. Autorità: Gli Influencer Nascosti

**Confronto tra:**
- **Centralità di Grado** (Volume)
- **PageRank** (Autorità)

#### Tre Categorie Identificate:

**1. I Re del Volume**
- **James Brown** domina entrambe le metriche
- 183 eventi di sampling + PageRank 11.70
- Inequivocabilmente il "Padrino del Sampling"

**2. I Ponti Strutturali**
- **Daft Punk** e **Beastie Boys**
- PageRank alto relativo al loro in-degree
- Agiscono come "ponti" tra generi
- Assorbono influenza dal passato e la trasmettono ad artisti moderni altamente connessi

**3. L'Autorità Inaspettata - Koji Kondo (近藤浩治)**

**La Scoperta Più Sorprendente del Progetto:**

**Chi è Koji Kondo?**
- Compositore di videogiochi Nintendo
- Creatore delle colonne sonore di:
  - Super Mario Bros.
  - The Legend of Zelda

**Posizione:** Rank #8 con PageRank 6.15

**Significato:** Rivela una genealogia non tradizionale:
> La musica dei videogiochi degli anni '80-'90 è stata ampiamente campionata nell'hip hop moderno

**Implicazioni:**
- Dimostra influenza cross-genre oltre la musicologia convenzionale
- Gli artisti hip hop hanno campionato temi iconici 8-bit
- L'estetica dei videogiochi è diventata parte della palette sonora dell'hip hop

**Path Genealogico:**
```
Videogiochi (anni '80) → Musica Elettronica → Hip Hop
```

### 4.4 Reti di Genealogia Musicale

Per visualizzare le relazioni reali "chi ha campionato chi", sono stati creati diagrammi di rete.

#### Figura 1: Rete di Sampling dei Top 15 Artisti

**Caratteristiche:**
- Solo i 15 artisti più influenti (ridotto da 20 per chiarezza)
- Solo connessioni forti (peso ≥ 3)
- **Risultato:** 9 relazioni chiave (riduzione del 96% rispetto a 244!)

**Interpretazione:**
- James Brown come hub centrale
- Artisti hip hop formano lo strato di sampling
- Visualizza la struttura genealogica del core

#### Figura 2: James Brown - Ego Network

**Cosa mostra:**
- James Brown al centro (nodo oro)
- 20 artisti principali che lo hanno campionato
- Layout circolare perfetto (nessuna sovrapposizione!)

**Perché è importante:**
Spiega visivamente **perché** James Brown ottiene il punteggio PageRank più alto (11.70).

**Artisti nella sua sfera di influenza:**
- Public Enemy
- LL Cool J
- Eric B. & Rakim
- E molti altri artisti hip hop influenti

**Concetto:** Sfera di influenza massiva

#### Figura 3: Koji Kondo - Ego Network

**Cosa mostra:**
- Koji Kondo al centro (nodo arancione)
- 15 artisti che hanno campionato la sua musica di videogiochi
- Layout circolare perfetto

**Significato:**
- Rappresenta una genealogia cross-genre unica
- Raramente studiata nella musicologia tradizionale
- Dimostra come l'8-bit è entrato nella produzione musicale moderna

**Quote Chiave:**
> "L'appeal nostalgico dei videogiochi degli anni '80-'90 ha influenzato una generazione di produttori cresciuti con questi suoni"

#### Figura 4: Analisi del Flusso di Sampling

**Visualizzazione bipartita:**
- **Lato sinistro:** Top 10 artisti che campionano pesantemente (alto out-degree)
- **Lato destro:** Top 10 artisti più campionati (autorità, alto in-degree)

**Spessore delle linee:** Frequenza di sampling

**Cosa rivela:**
- Divisione temporale e di genere
- Lato destro: Principalmente funk/soul/rock anni '70-'80 (sorgenti)
- Lato sinistro: Principalmente hip hop/elettronica anni '90-2000 (campionatori)

**Artisti su entrambi i lati:**
- Esempio: Beastie Boys
- Agiscono come ponti evolutivi

#### Figura 5: Classificazione degli Artisti - Analisi degli Hub

**Scatter plot:** In-degree (asse Y) vs Out-degree (asse X)

**Quadranti identificati:**

1. **Autorità Pure** (quadrante superiore-sinistro)
   - Alto in-degree, basso out-degree
   - Esempi: James Brown, Marvin Gaye
   - Sono il "materiale sorgente" della musica moderna

2. **Ponti** (quadrante superiore-destro, rari)
   - Alto sia in-degree che out-degree
   - Artisti che sia campionano pesantemente CHE vengono campionati
   - Nodi evolutivi che trasformano l'influenza

3. **Campionatori Pesanti** (quadrante inferiore-destro)
   - Basso in-degree, alto out-degree
   - Produttori moderni che si affidano estensivamente al sampling
   - Non sono ancora diventati autorità loro stessi

**Validazione:**
La forte correlazione tra in-degree e PageRank (punti rossi raggruppati in alto a sinistra) valida l'algoritmo PageRank:
> L'autorità deriva principalmente dall'essere campionati da artisti influenti

### 4.5 Rilevamento di Comunità e Famiglie Genealogiche

**Risultati dell'Algoritmo Label Propagation:**
- **8,140 cluster distinti** rilevati
- **Modularity score:** 0.2657

**Interpretazione della Modularità:**
- 0.2657 indica struttura di comunità **moderata**
- La rete musicale non è né completamente frammentata né completamente connessa
- Esibisce famiglie genealogiche significative

**Analisi dei Cluster:**

| Cluster | Dimensione (canzoni) | Interpretazione |
|---------|---------------------|-----------------|
| Più grande | 90 | Lineage genealogica dominante (probabilmente funk/soul → hip hop) |
| 2° più grande | 52 | Movimento musicale principale |
| 3° più grande | 38 | Famiglia genealogica secondaria |
| Top 5 totale | 221 | Le 5 famiglie principali |

**8,140 cluster totali:**
- Molti cluster piccoli rappresentano catene di sampling isolate
- Pattern: A → B → C
- Non ancora connesse alla rete principale

**Spiegazione della Modularità 0.2657:**
- Le comunità sono distinguibili ma non completamente isolate
- Atteso nella musica dove i generi si fondono

### 4.6 Validazione Power-Law

**Distribuzione cumulativa del grado (scala log-log):**

**Conferma dell'ipotesi di rete scale-free:**
- Il trend lineare nello spazio log-log conferma che la rete segue una distribuzione power-law

**Analisi di Concentrazione:**
- Top 1%: 18.7% di tutti gli eventi di sampling
- Top 5%: 38.7% di tutti gli eventi di sampling
- Top 10%: 50.4% di tutti gli eventi di sampling

**Fenomeno "I Ricchi Diventano Più Ricchi":**

**Spiegazione:**
Artisti consolidati come James Brown accumulano eventi di sampling a un tasso proporzionale alla loro influenza esistente.

**Termine tecnico:** **Preferential Attachment**

**Caratteristico di:**
- Reti sociali reali
- Reti di influenza
- World Wide Web
- Reti di citazioni accademiche

---

## 5. Discussione e Interpretazione

### 5.1 Implicazioni Teoriche

**Dimostrazione chiave:**
L'influenza musicale è fondamentalmente un **fenomeno di rete** governato da proprietà strutturali piuttosto che solo metriche di popolarità.

#### Tre Insight Chiave:

**1. Autorità vs. Popolarità**

**PageRank rivela:**
> La posizione strutturale (essere campionati da artisti influenti) conta tanto quanto il volume grezzo

**Esempio:**
- Koji Kondo ha meno campioni totali di molti altri artisti
- MA è campionato da artisti influenti
- QUINDI rank più alto di artisti con più campioni totali

**2. Genealogia Cross-Genre**

**Scoperta:**
La comparsa della musica di videogiochi nell'hip hop

**Dimostrazione:**
Il sampling crea percorsi genealogici inaspettati oltre i confini di genere tradizionali

**Conclusione:**
L'evoluzione musicale non è lineare ma **reticolare**

**3. Preferential Attachment**

**Distribuzione power-law conferma:**
L'influenza musicale segue gli stessi pattern matematici di:
- Reti di citazioni
- Reti sociali
- World Wide Web

**Pattern:** Pochi hub dominano mentre la maggior parte dei nodi rimane periferica

### 5.2 Contributi Metodologici

#### 1. Implementazione From-Scratch

**Cosa:** Implementare PageRank e Label Propagation manualmente in PySpark

**Perché è importante:**
- Dimostra comprensione profonda degli algoritmi distribuiti su grafo
- Va oltre l'uso di librerie pre-costruite (es. GraphFrames)

**Valore:** Mostra capacità di ingegneria algoritmica

#### 2. Data Engineering

**Sfida:** Navigare lo schema relazionale complesso di MusicBrainz (Terza Forma Normale)

**Soluzione:** Trasformarlo in una struttura a grafo

**Dimostra:**
- Competenze di data engineering nel mondo reale
- Capacità di lavorare con dati "sporchi" e non strutturati
- Non solo dataset pre-puliti di Kaggle

#### 3. Ottimizzazione della Convergenza

**Implementazione:**
- Criteri di convergenza con stopping adattivo
- Convergenza raggiunta in solo 11 iterazioni
- Tolleranza < 0.0001

**Efficienza:** Molto meglio di un numero fisso arbitrario di iterazioni

#### 4. Strategia di Visualizzazione

**Creazione di visualizzazioni di rete pulite e non sovrapposte:**

**Tecniche:**
- Layout circolare per ego network (nessuna sovrapposizione!)
- Layout bipartito per diagrammi di flusso
- Riduzione delle connessioni mostrate (244 → 9 archi)

**Risultato:** Strutture complesse diventano interpretabili

### 5.3 Limitazioni e Lavoro Futuro

#### Limitazione 1: Qualità dei Dati

**Problema identificato:**
- Presenza di "Ninja McTits"
- Artista placeholder/test con PageRank anomalo (66.45)

**Indica:** Problemi di qualità dei dati in MusicBrainz

**Lavoro futuro:**
- Implementare rilevamento automatico di outlier
- Pipeline di pulizia dati più robusta

#### Limitazione 2: Analisi Temporale

**Stato attuale:** L'analisi è statica

**Mancanza:** Informazione temporale (date di rilascio)

**Lavoro futuro:**
- Incorporare date di rilascio
- Analisi di rete temporale
- Mostrare come i pattern di sampling si sono evoluti nei decenni

**Domande che potrebbe rispondere:**
- Come è cambiato il sampling dagli anni '70 ai 2000?
- Quali generi hanno influenzato quali in quale epoca?

#### Limitazione 3: Metadati di Genere

**Approccio attuale:** Label Propagation è completamente non supervisionato

**Opportunità:**
- Incorporare tag espliciti di genere
- Validazione: I cluster strutturali si allineano con le definizioni di genere convenzionali?

**Potrebbe rispondere:**
- I cluster algoritmici corrispondono ai generi musicali tradizionali?
- Quali generi sono più interconnessi strutturalmente?

#### Limitazione 4: Genealogia Multi-Hop

**Analisi attuale:** Relazioni dirette di sampling

**Estensione:**
- Tracciare catene di sampling multi-generazionali
- Esempio: Originale (1970) → Campione 1 (1985) → Ri-campione (2000)

**Rivelerebbe:** Percorsi genealogici più profondi

---

## 6. Conclusione

### Sintesi del Progetto

Questo progetto ha applicato con successo tecniche di mining distribuito su grafo per scoprire la genealogia nascosta della musica moderna.

**Metodo:**
1. Modellare le relazioni di sampling come grafo diretto
2. Implementare PageRank e Label Propagation da zero in Apache Spark
3. Rivelare pattern strutturali invisibili alle metriche tradizionali basate sul volume

### Risultati Chiave

#### 1. James Brown - Il Padrino Indiscusso
- **PageRank:** 11.70 (più alto)
- **In-Degree:** 183 eventi di sampling
- **Ego Network:** 20+ artisti principali nella sua sfera di influenza

**Interpretazione:**
> Il dominio strutturale di James Brown nella rete di sampling giustifica il suo titolo di "Padrino del Soul"

#### 2. Koji Kondo - La Scoperta Cross-Genre
- **Rank:** #8
- **PageRank:** 6.15
- **Significato:** Musica di videogiochi → Hip hop

**Impatto:**
> Dimostra che il sampling crea genealogie musicali che trascendono i confini di genere tradizionali

#### 3. Rete Scale-Free Confermata
- **Distribuzione power-law:** Confermata
- **Concentrazione:** Top 1% = 18.7%, Top 10% = 50.4%

**Validazione:**
> Il pattern di influenza musicale segue le stesse leggi matematiche delle reti sociali e di citazione

#### 4. Famiglie Genealogiche
- **8,140 cluster** rilevati
- **Modularità:** 0.2657 (struttura di comunità moderata)

**Interpretazione:**
> I generi musicali sono comunità di pratica identificabili algoritmicamente

#### 5. Convergenza Efficiente
- **11 iterazioni** per convergenza PageRank
- **Tolleranza:** < 0.0001

**Validazione:**
> L'implementazione ottimizzata funziona efficientemente

### Cambio di Paradigma

**Da:**
> "Chi ha venduto più dischi?"

**A:**
> "Chi ha strutturalmente plasmato la musica moderna?"

**Metodo:**
> Sfruttando la teoria dei grafi e il calcolo distribuito

**Risultato:**
> Abbiamo mappato il DNA del suono contemporaneo

**Rivelazione:**
> L'influenza fluisce attraverso reti, non classifiche

### Impatto Potenziale

Questa metodologia potrebbe estendersi ad altri domini:

**1. Reti di Citazioni Accademiche**
- Domanda: Quali paper sono fondamentali?
- Analogia: Chi ha "campionato" (citato) chi?

**2. Influenza sui Social Media**
- Domanda: Chi guida la conversazione?
- Analogia: Chi amplifica (ri-tweeta/condivide) chi?

**3. Dipendenze Software**
- Domanda: Quali librerie sono alla base dello sviluppo moderno?
- Analogia: Quali pacchetti dipendono da quali?

**Universalità:**
> Gli strumenti sono universali; gli insight sono profondi

---

## Glossario dei Termini Tecnici

### Termini di Network Science

**Grafo Diretto (Directed Graph):**
- Grafo dove gli archi hanno una direzione
- Esempio: A → B (A campiona B)

**Nodo (Node/Vertex):**
- Entità nel grafo
- Nel nostro caso: canzoni

**Arco (Edge):**
- Connessione tra nodi
- Nel nostro caso: relazione di sampling

**In-Degree:**
- Numero di archi entranti in un nodo
- Misura: quante volte un artista viene campionato

**Out-Degree:**
- Numero di archi uscenti da un nodo
- Misura: quanti campioni usa un artista

**Densità del Grafo:**
- Rapporto tra archi esistenti e archi possibili
- Valore basso = rete sparsa

**Componente Connessa:**
- Sottografo dove tutti i nodi sono raggiungibili tra loro
- Nel nostro caso: famiglie genealogiche

**Rete Scale-Free:**
- Rete con distribuzione di grado power-law
- Pochi hub, molti nodi periferici

**Preferential Attachment:**
- I nodi con più connessioni tendono ad acquisirne ancora di più
- "I ricchi diventano più ricchi"

### Termini di Algoritmi

**PageRank:**
- Algoritmo che misura l'importanza dei nodi in un grafo
- Basato sulla qualità delle connessioni, non solo quantità

**Damping Factor:**
- Parametro PageRank (tipicamente 0.85)
- Probabilità di seguire un arco vs. saltare random

**Convergenza:**
- Quando un algoritmo iterativo raggiunge stabilità
- Cambiamenti tra iterazioni diventano trascurabili

**Label Propagation:**
- Algoritmo di clustering non supervisionato
- I nodi adottano etichette dei vicini

**Modularity:**
- Misura della qualità del clustering
- Valore alto = comunità ben definite

**Power-Law Distribution:**
- Distribuzione dove P(x) ∝ x^(-α)
- Caratteristica delle reti scale-free

### Termini di Big Data

**Apache Spark:**
- Framework per processing distribuito di big data
- Elaborazione in-memory veloce

**PySpark:**
- API Python per Apache Spark
- Permette di scrivere job Spark in Python

**Lazy Evaluation:**
- Spark non esegue subito le trasformazioni
- Costruisce un piano di esecuzione ottimizzato

**DataFrame:**
- Struttura dati distribuita in Spark
- Simile a tabella SQL

**Parquet:**
- Formato di storage colonnare compresso
- Ottimizzato per query analitiche

**Schema-on-Read:**
- Definire lo schema quando si leggono i dati
- Vs schema-on-write (definire prima di scrivere)

**Checkpoint:**
- Salvare stato intermedio su disco
- Tronca la lineage di Spark

**MapReduce:**
- Paradigma di programmazione distribuita
- Map (trasforma) + Reduce (aggrega)

### Termini Musicali

**Sampling:**
- Riutilizzare una porzione di una registrazione esistente in una nuova canzone
- Base dell'hip hop e della musica elettronica

**Original Artist:**
- Artista la cui musica viene campionata
- La "sorgente" nella rete di influenza

**Sampler:**
- Artista che campiona la musica di altri
- Il "ricevente" nella rete di influenza

**One-Hit Wonder:**
- Artista con un solo grande successo
- Alto volume ma basso impatto strutturale

**Cross-Genre:**
- Che attraversa i confini dei generi musicali
- Esempio: Video game → Hip hop

---

## Domande Frequenti (FAQ)

### Q1: Perché PageRank è meglio di un semplice conteggio?

**A:** 
Un semplice conteggio (in-degree) tratta tutti i campioni allo stesso modo.

**Esempio:**
- Artista A: campionato 10 volte da artisti sconosciuti
- Artista B: campionato 5 volte da superstar

**In-Degree dice:** A è più influente (10 > 5)

**PageRank dice:** B è più influente (qualità > quantità)

**Perché?** Essere campionato da una superstar che ha grande influenza trasmette più autorità.

### Q2: Come funziona Label Propagation in parole semplici?

**A:**
Immagina una scuola dove:
1. Ogni studente inizia con un'etichetta unica (il proprio nome)
2. Ogni giorno, ogni studente adotta l'etichetta del suo migliore amico
3. Dopo alcuni giorni, gruppi di amici condividono la stessa etichetta
4. Questi gruppi = comunità rilevate

**Nella rete musicale:**
- Artisti che campionano le stesse sorgenti convergono verso la stessa etichetta
- = Stessa famiglia genealogica

### Q3: Perché usare Spark invece di Pandas?

**A:**

**Pandas:**
- Ottimo per dati che stanno in memoria su una singola macchina
- ~Milioni di righe ok

**Spark:**
- Necessario quando:
  - Dati troppo grandi per la memoria di una macchina
  - Join complessi su dataset enormi
  - Algoritmi iterativi (come PageRank)

**Nel nostro caso:**
- 22K archi, 30K nodi = tecnicamente possibile con Pandas
- MA: Join multipli + algoritmo iterativo = molto più efficiente con Spark
- Plus: Dimostra competenza con strumenti enterprise

### Q4: Qual è l'insight più sorprendente del progetto?

**A:** **Koji Kondo al rank #8**

**Perché sorprendente:**
1. È un compositore di videogiochi, non un musicista "tradizionale"
2. La sua musica 8-bit è stata campionata nell'hip hop
3. Nessuno si aspetterebbe Mario Bros in una rete di influenza musicale

**Cosa rivela:**
- Il sampling crea genealogie inaspettate
- La cultura pop (gaming) influenza la musica mainstream
- I generi non sono isolati

### Q5: Cosa significa "rete scale-free"?

**A:**

**Definizione semplice:**
Una rete dove la maggior parte dei nodi ha poche connessioni, ma alcuni nodi (hub) hanno MOLTE connessioni.

**Visualizza:**
```
Normale:   ●-●-●-●-●  (tutti simili)
Scale-Free:   ●
            / | \
           ●  ●  ●-●-● (un hub, molti periferici)
```

**Esempi:**
- Internet: pochi siti molto linkati (Google), molti siti poco linkati
- Aeroporti: pochi hub (Atlanta, Londra), molti aeroporti piccoli
- Musica: pochi artisti molto campionati (James Brown), molti poco campionati

**Matematica:** Distribuzione power-law (P(k) ∝ k^-α)

### Q6: Perché 11 iterazioni sono sufficienti?

**A:**

**PageRank è iterativo:**
1. Inizia: tutti hanno lo stesso punteggio
2. Iterazione 1: redistribuisce basandosi sulle connessioni
3. Iterazione 2: aggiusta ancora
4. ... continua ...
5. Iterazione 11: cambiamenti < 0.0001 (convergenza!)

**Perché si ferma:**
- I punteggi si stabilizzano
- Ulteriori iterazioni non cambiano significativamente i risultati

**Efficienza:**
- Convergenza automatica > numero fisso
- Risparmia computazione inutile

### Q7: Cosa significa modularità 0.2657?

**A:**

**Scala della Modularità:**
- **0.0:** Nessuna struttura di comunità (tutto mescolato)
- **0.3-0.7:** Buona struttura di comunità
- **1.0:** Perfettamente separato (ideale, raro)

**0.2657 = Moderato**

**Interpretazione nella musica:**
- Le famiglie genealogiche esistono
- MA i confini non sono netti
- Artisti possono appartenere a più famiglie
- = Realistico! I generi musicali si sovrappongono

**Perché non più alto:**
- Sampling è cross-genre
- Artisti sperimentano
- Non ci sono muri tra generi

### Q8: Come posso replicare questo progetto?

**A:**

**Requisiti:**
1. Apache Spark installato
2. Python con PySpark
3. Dataset MusicBrainz (download gratuito)

**Step:**
1. Scarica MusicBrainz dump
2. Usa `data_preparation.py` per costruire il grafo
3. Esegui `compute_authority_manual.py` per PageRank
4. Esegui `cluster.py` per Label Propagation
5. Genera visualizzazioni con gli script forniti

**Tempo:**
- Setup: ~2 ore
- Elaborazione: ~30 minuti (dipende dall'hardware)
- Visualizzazioni: ~10 minuti

**Consulta:** `QUICKSTART.md` nel progetto per istruzioni dettagliate

---

## Riferimenti Rapidi

### File Chiave del Progetto

**Script Python:**
- `data_preparation.py` - Costruzione del grafo da MusicBrainz
- `compute_authority_manual.py` - Algoritmo PageRank
- `cluster.py` - Label Propagation
- `validation_metrics.py` - Statistiche della rete
- `cluster_quality.py` - Analisi della modularità
- `genealogy_visualizations.py` - Visualizzazioni di rete

**Dati:**
- `music_graph.parquet` - Grafo finale (20,728 archi, self-loop rimossi)
- `artist_pagerank.parquet` - Punteggi PageRank
- `music_labels.parquet` - Assegnazioni cluster
- `top_100_artists_pagerank.csv` - Top 100 ranking

**Visualizzazioni:**
- `report_figures/` - Grafici statistici
- `genealogy_networks/` - Diagrammi di rete

**Documentazione:**
- `README.md` - Panoramica del progetto
- `QUICKSTART.md` - Come eseguire
- `SPIEGAZIONE_ITALIANA.md` - Questo documento!

### Comandi Utili

**Eseguire il pipeline completo:**
```bash
cd dataset/
./run_pipeline.sh
```

**Generare solo figure:**
```bash
python generate_report_figures.py
python genealogy_visualizations.py
```

**Compilare il report:**
```bash
cd report/
pdflatex -shell-escape DataMining.tex
```

---

## Conclusione della Spiegazione

Questo progetto dimostra come tecniche avanzate di data science possono rivelare insight nascosti nell'ecosistema musicale:

**✓ Data Engineering** - Trasformare un database relazionale complesso in un grafo

**✓ Algoritmi Distribuiti** - Implementare PageRank e LPA da zero in Spark

**✓ Network Science** - Applicare teoria dei grafi a un problema del mondo reale

**✓ Visualizzazione** - Creare diagrammi interpretatibili di strutture complesse

**✓ Insight Unici** - Scoprire connessioni inaspettate (video games → hip hop)

**Risultato:** Un'analisi publication-ready che va oltre le classifiche di popolarità per mappare la vera genealogia della musica moderna.

---

**Buona fortuna con la tua presentazione! 🎓🎵**
