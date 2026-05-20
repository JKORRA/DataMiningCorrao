# Spiegazione Facile del Report di Data Mining

## Indice
1. [Introduzione — Di cosa parla il progetto?](#1-introduzione--di-cosa-parla-il-progetto)
2. [Preparazione dei Dati](#2-preparazione-dei-dati)
3. [Metodologia e Algoritmi](#3-metodologia-e-algoritmi)
4. [Risultati](#4-risultati)
5. [Discussione](#5-discussione)
6. [Conclusione](#6-conclusione)

---

## 1. Introduzione — Di cosa parla il progetto?

### Il problema
Nell'industria musicale, di solito misuriamo il successo con **vendite e streaming** (cioè la *popolarità*). Ma una canzone famosa non è necessariamente importante dal punto di vista culturale. Per esempio:
- Una **one-hit wonder** può vendere milioni di copie e poi sparire.
- Un **vecchio pezzo funk degli anni '70** può essere campionato (riusato) da centinaia di artisti hip-hop, diventando la base di un intero genere musicale.

> **Obiettivo del progetto**: spostare l'analisi dal *volume* (quanto sei famoso) alla *struttura* (quanto sei influente).

### Il grafo
Per farlo, il progetto costruisce un **Grafo Diretto**:
- **Nodi** (pallini) = **canzoni** (67.638 canzoni uniche)
- **Archi** (frecce) = **rapporti di campionamento** → una freccia dice "la canzone X ha campionato la canzone Y"

Ogni canzone appartiene a un **artista**. Quindi alla fine l'analisi viene fatta a livello di **artista** (23.242 artisti unici).

### Cosa vogliamo scoprire
1. **Chi sono gli antenati strutturali** della musica moderna (Authority)?
2. **Quali comunità di influenza** esistono (cluster)?
3. **Come fluisce l'influenza** usando algoritmi distribuiti (Spark)?

---

## 2. Preparazione dei Dati

### 2.1 Fonte dei dati: MusicBrainz

**MusicBrainz** è un'enciclopedia musicale aperta (tipo Wikipedia per la musica). La gente ci inserisce informazioni su artisti, canzoni, album, ecc.

La differenza rispetto a dataset semplici (tipo quelli di Kaggle):
- MusicBrainz è **altamente normalizzato** (3NF = Terza Forma Normale). Significa che i dati sono sparsi in tante tabelle collegate tra loro da ID numerici, non leggibili direttamente.
- Per ricostruire il significato serve un lavoro di **data engineering** importante.

### 2.2 Acquisizione e ingegneria dei dati

I dati grezzi sono file **TSV** (valori separati da tabulazione) senza intestazioni, estratti da un archivio chiamato `mbdump.tar.bz2`.

Il processo per trasformarli in un grafo strutturato ha tre fasi:
1. **Definire lo schema** (dire al computer che tipo di dato è ogni colonna)
2. **Proiettare** (selezionare solo le colonne che ci servono)
3. **Ricostruire il grafo** (unire le tabelle per ottenere "Artista X campiona Artista Y")

### 2.3 Perché Apache Spark?

Perché non usare semplicemente Pandas o Excel? Tre motivi:

1. **Elaborazione distribuita**: Spark può dividere il lavoro su più computer (o core) in parallelo.
2. **Lazy Evaluation**: Spark non esegue subito le operazioni, prima costruisce un "piano di esecuzione" ottimizzato.
3. **Calcolo iterativo**: algoritmi come PageRank richiedono molte iterazioni, ideali per Spark.

### 2.4 Schema-on-Read

I file TSV non hanno intestazioni. Se lasciamo che Spark indovini i tipi di dato da solo, è lento e rischioso. Invece, **definiamo manualmente lo schema** di ogni tabella leggendo la documentazione di MusicBrainz.

Esempio: per la tabella `recording` (canzoni) diciamo:
- `id` = numero intero
- `name` = testo (il nome della canzone)
- `artist_credit` = numero intero (l'ID dell'artista)
- ... eccetera

### 2.5 Filtraggio dei dati

Dopo aver caricato i dati, due operazioni:

**1. Colonne essenziali**: teniamo solo ID e nomi, buttiamo via metadati inutili (tipo "commenti" o "edits_pending"). Questo riduce la memoria necessaria.

**2. Solo campionamenti veri**: la tabella `l_recording_recording` contiene *milioni* di relazioni tra canzoni: cover, remix, medley, performance dal vivo, e **campionamenti**. Se includessimo tutto, il grafo sarebbe rumoroso e confuso.

Per isolare solo i **campionamenti** (cioè quando un pezzo audio viene fisicamente riusato), abbiamo cercato nella tabella `link_type` gli ID giusti. Abbiamo trovato:
- **ID 69**: "Samples material" (tipo legacy, più vecchio)
- **ID 231**: "Is sampled by" (tipo corrente)

Quindi nel grafo finale **teniamo solo gli archi con questi ID**, filtrando via tutto il rumore.

### 2.6 Self-Loop Removal — Passaggio CRUCIALE

Durante l'analisi esplorativa, abbiamo scoperto un problema: ci sono **self-loop**, cioè archi dove un artista campiona *se stesso*.

#### I numeri
- **1.407 self-loop** trovati
- Sono circa il **6.4%** di tutti gli archi

#### Esempi di self-loop
- Un artista che fa un **remix** della propria canzone
- **Ripubblicazioni** (versione album vs versione singolo)
- **Struttura dell'album** (traccia intro che campiona un'altra traccia dello stesso album)
- **Errori nei dati** (doppioni nel database)

#### Perché sono un problema?

Due motivi:

1. **Inflazione del PageRank**: un artista con tanti self-loop si "autovota". Esempio:
   - Un artista chiamato "Ninja McTits" aveva ben **443 self-loop**
   - Il suo PageRank era **66.45** — più alto di **James Brown** (45.67), che è il legittimo artista più influente!
   - Se non rimuoviamo i self-loop, la classifica è falsata.

2. **Genealogia sbagliata**: un self-loop non rappresenta l'influenza tra artisti diversi. È "riciclo interno", non trasmissione di idee tra creatori diversi.

#### La soluzione
Rimuoviamo tutti gli archi dove:
```
Sampler_Artist_Name = Original_Artist_Name
```
(cioè "se chi campiona è lo stesso artista di chi viene campionato, elimina l'arco")

#### Attenzione ai due numeri! (spiegazione importante)

Dopo aver rimosso i self-loop, se **collassiamo** gli archi a livello di artista (cioè: se tra Artista A e Artista B ci sono 5 canzoni diverse campionate, le contiamo come un unico arco con peso 5), otteniamo:
- **20.728 archi** a livello **artista→artista**

Ma se manteniamo la **granularità fine** (ogni singola canzone che campiona un'altra canzone):
- **57.741 archi** a livello **canzone→canzone**

Entrambi i numeri sono corretti! Dipende dal livello di dettaglio:
- 57.741 = "quante volte un campionamento è avvenuto" (canzoni)
- 20.728 = "quante coppie di artisti sono collegate" (artisti, conteggiati una volta sola)

Il report nel testo usa **20.728** per parlare della topologia artista-artista, e **57.741** per parlare degli eventi di campionamento totali.

### 2.7 Name Normalization e Alias Mapping

#### Il problema
MusicBrainz memorizza gli artisti con nomi diversi a seconda di come sono accreditati sulla pubblicazione originale. Lo stesso artista può apparire come:
- "James Brown" (nome principale)
- "James Brown & The Famous Flames" (con il gruppo)
- "JAMES BROWN" (variazione maiuscole)
- "James Brown feat. Lyn Collins" (con featuring)

Senza normalizzazione, questi vengono trattati come **artisti diversi**, frammentando il grafo.

#### La soluzione: Alias Map automatico
Abbiamo costruito una mappa di alias che riconosce quando un nome è una variante di un altro. L'algoritmo funziona in due fasi:

1. **Prefix Match**: il nome candidato deve iniziare con il nome principale (es. "James Brown & The Famous Flames" inizia con "James Brown").
2. **Suffix Validation**: la parte rimanente (suffisso) deve iniziare con una congiunzione (&, and, feat.) oppure essere un qualificatore singolo (Trio, Quartet, Quintet) o una frase che termina con un qualificatore noto.

Risultato: **105 alias per prefisso** + **56 varianti di maiuscole/minuscole** = **161 alias totali**.

Esempi di alias rilevati:
- "James Brown & The Famous Flames" → "James Brown"
- "James Brown and the Famous Flames" → "James Brown"
- "john lennon" → "John Lennon"
- "2pac" (variazione minuscola) → "2Pac"

L'alias map viene applicata **prima** degli arricchimenti (join con recording e artist_credit), in modo che tutti i nomi siano già normalizzati quando il grafo viene ricostruito. Questo assicura che artisti come "James Brown & The Famous Flames" vengano correttamente fusi con "James Brown" nel grafo finale.

### 2.8 Ricostruzione del grafo

MusicBrainz è normalizzato: un campionamento è memorizzato come **due ID numerici** (`entity0` = canzone sorgente, `entity1` = canzone target), senza nome della canzone o dell'artista.

Per ottenere un grafo leggibile (tipo "Artista A ha campionato Artista B"), dobbiamo fare una serie di **join** (unioni tra tabelle):

1. **Collegamento**: uniamo `l_recording_recording` con la tabella `link` per filtrare solo gli ID 69 e 231.
2. **Arricchimento sorgente**: uniamo con `recording` e `artist_credit` per avere il nome della canzone sorgente e dell'artista che ha campionato.
3. **Arricchimento target**: stesso join per la canzone target (quella campionata) e il suo artista.

Alla fine: una tabella leggibile con "Sampler → Original".

### 2.9 Ottimizzazione: salvataggio in Parquet

Costruire il grafo richiede join su milioni di righe. È costoso. Per non doverlo rifare ogni volta, salviamo il grafo finale in formato **Apache Parquet**.

Perché Parquet è meglio di CSV?
1. **Storage colonnare**: se servono solo certe colonne, legge solo quelle (più veloce).
2. **Preserva lo schema**: ricorda i tipi di dato (interi, testi) — non serve re-impostarli.
3. **Compressione**: occupa meno spazio su disco.

---

## 3. Metodologia e Algoritmi

Con il grafo costruito e salvato, l'analisi si sviluppa su **tre livelli** di complessità crescente:
1. Statistiche descrittive (grado)
2. Autorità strutturale (PageRank)
3. Comunità (Clustering)

**Importante**: Il PageRank è implementato **da zero** in PySpark, mentre il community detection usa l'implementazione **Louvain** della libreria **NetworkX**. Questa combinazione dimostra competenza sia negli algoritmi distribuiti che nell'analisi di reti.

### 3.1 Analisi descrittiva: Degree Centrality

Il **grado** è la misura più semplice in un grafo.

**In-Degree** (grado entrante) = quante volte un artista viene campionato.
- Misura la **popolarità** come fonte di materiale.
- Esempio: se James Brown viene campionato 203 volte, il suo In-Degree è 203.

**Out-Degree** (grado uscente) = quanti campionamenti fa un artista.
- Identifica gli artisti che **usano più campionamenti** nel loro lavoro.

Implementazione: una semplice operazione `groupBy("Artista").count()` — Spark conta quante volte ogni artista appare come "Originale" (In-Degree) o come "Sampler" (Out-Degree).

### 3.2 PageRank — L'autorità strutturale

#### Il problema del semplice conteggio
"In-Degree" conta *quante* volte un artista è campionato, ma non *da chi*. Se un artista è campionato da 10 artisti sconosciuti, ha lo stesso In-Degree di un artista campionato da 10 superstar. Ma l'influenza è diversa!

#### La soluzione: PageRank
PageRank risolve questo problema: dice che **essere campionato da un artista influente trasmette più autorità che essere campionato da uno sconosciuto**.

#### La formula (semplificata)
```
PR(A) = (1 - d) + d × (PR(B₁)/Out(B₁) + PR(B₂)/Out(B₂) + ...)
```
Dove:
- **PR(A)** = PageRank dell'artista A
- **d** = damping factor (0.85 = valore standard) — rappresenta la probabilità che un "navigatore" segua un link invece di iniziare da capo
- **PR(B₁)** = PageRank dell'artista B₁ (che campiona A)
- **Out(B₁)** = numero di artisti che B₁ campiona in totale

Il significato: l'autorità di A è data dalla somma delle autorità di chi lo campiona, "divise" per quanti altri artisti ciascuno di loro campiona.

#### Implementazione in PySpark
L'algoritmo è **iterativo**: ripete lo stesso calcolo tante volte finché i numeri non smettono di cambiare.

- L'arco va da **Sampler → Originale** (chi campiona → chi viene campionato), perché l'autorità fluisce da chi campiona a chi viene campionato.
- L'algoritmo usa **50 iterazioni fisse** con checkpointing per prevenire errori di memoria e corretta redistribuzione dei dangling nodes.

#### Due problemi tecnici risolti

1. **Dangling Nodes (nodi senza uscita)**: alcuni artisti sono campionati ma non campionano mai nessuno. Sono "buchi neri" per il PageRank (l'autorità non può uscire). Li identifichiamo con un `right_outer_join` e ridistribuiamo i loro punteggi.

2. **Lineage Truncation**: Spark costruisce un "albero genealogico" delle operazioni. Dopo tante iterazioni, quest'albero diventa enorme e causa errori di memoria. Usiamo `.checkpoint()` per salvare lo stato intermedio su disco e "troncare" l'albero.

### 3.3 Louvain — Rilevamento comunità

Per trovare le "famiglie musicali", usiamo l'algoritmo **Louvain**, un metodo di community detection basato sulla massimizzazione della **modularità**.

#### Come funziona

L'algoritmo opera in due fasi, ripetute fino a convergenza:

1. **Fase 1 — Ottimizzazione locale**: ogni nodo viene spostato nella comunità del vicino che massimizza l'aumento di modularità. Si ripete finché nessuno spostamento produce un miglioramento.

2. **Fase 2 — Aggregazione**: le comunità trovate vengono "compresse" in super-nodi, e si costruisce un nuovo grafo pesato che rappresenta le connessioni tra comunità. Poi la Fase 1 ricomincia su questo grafo aggregato.

Il processo continua fino a quando non è più possibile migliorare la modularità.

#### Dettagli implementativi
- Usiamo l'implementazione di **NetworkX** (`community.louvain_communities`) con risoluzione **r = 1.0**
- Il grafo è **non diretto e pesato**: ogni arco diretto (A campiona B) viene trattato come arco non diretto con peso 1 (o conteggio cumulativo)
- I self-loop sono esclusi

#### Perché è interessante
L'algoritmo **non sa niente di generi musicali**. Non sa cos'è "Hip Hop" o "Rock". Funziona solo sulla **struttura** (chi campiona chi). Se un gruppo di artisti emerge come cluster, significa che condividono pratiche di campionamento — e questo *di solito* corrisponde a un genere musicale.

---

## 4. Risultati

### 4.1 Statistiche di Rete e Validazione

#### I numeri principali del grafo finale
Dopo pulizia e rimozione self-loop:

| Cosa | Numero |
|------|--------|
| Eventi di campionamento (archi) | **57.741** |
| Canzoni uniche (nodi) | **67.638** |
| Artisti unici | **23.242** |
| Artisti che campionano | 9.670 |
| Artisti che VENGONO campionati | 16.390 |
| **Densità del grafo** | **0.00001262** |

#### Cosa significa "densità 0.00001262"?

La densità misura quanto un grafo è "connesso". Va da 0 (nessuna connessione) a 1 (tutti connessi a tutti). Un valore così piccolo (0.00001262) dice che la rete è **molto sparsa**, come ci si aspetta in una rete d'influenza reale. Non tutti campionano tutti — solo poche connessioni significative.

#### Power-Law Distribution

La rete mostra un comportamento **scale-free** (senza scala), tipico delle reti sociali, delle citazioni accademiche e del web. I numeri della concentrazione lo confermano:
- **Top 1%** degli artisti → **23.5%** di tutti i campionamenti
- **Top 5%** degli artisti → **46.6%** di tutti i campionamenti
- **Top 10%** degli artisti → **58.3%** di tutti i campionamenti

Significa che pochi hub dominano e la maggior parte dei nodi è periferica. L'influenza musicale segue l'attaccamento preferenziale: gli artisti già famosi tendono ad essere campionati sempre di più.

#### Statistiche sul grado

| Metrica | In-Degree (volte campionato) | Out-Degree (campionamenti fatti) |
|---------|------------------------------|----------------------------------|
| Media | **3.52** | **5.97** |
| Deviazione standard | 10.54 | 32.45 |
| Massimo | **370** (Daft Punk) | **1.488** |

- La media In-Degree è 3.52: un artista tipico viene campionato 3-4 volte
- La media Out-Degree è 5.97: un artista tipico usa circa 6 campionamenti
- La deviazione standard alta conferma la disuguaglianza (pochi hanno valori molto alti)
- Max Out-Degree è 1.488: c'è un artista che ha campionato quasi 1.500 altri artisti!

---

### 4.2 Classifica PageRank (Authority Rankings)

Il PageRank è stato eseguito con **50 iterazioni fisse** con monitoraggio della convergenza e corretta redistribuzione dei dangling nodes.

#### Tabella: Top 10 per Volume vs Authority

**In-Degree (Volume)** — Chi è più campionato in termini assoluti:

| Rank | Artista | Conteggio |
|------|---------|-----------|
| 1 | Daft Punk | 370 |
| 2 | Lady Gaga | 281 |
| 3 | The Beatles | 267 |
| 4 | JAY-Z | 260 |
| 5 | PSY | 253 |
| 6 | Linkin Park | 230 |
| 7 | Michael Jackson | 220 |
| 8 | Beastie Boys | 209 |
| 9 | James Brown | 203 |
| 10 | Katy Perry | 186 |

**PageRank (Authority)** — Chi è più influente strutturalmente:

| Rank | Artista | Punteggio |
|------|---------|-----------|
| 1 | **Daniel Ingram** | 59.85 |
| 2 | **James Brown** | 45.67 |
| 3 | **2Pac** | 45.01 |
| 4 | **電音部 (Denonbu)** | 40.90 |
| 5 | **外神田文芸高校** | 35.61 |
| 6 | Lyn Collins | 27.10 |
| 7 | Daft Punk | 27.08 |
| 8 | **開発室Pixel** | 26.98 |
| 9 | John Lennon | 26.06 |
| 10 | Kraftwerk | 25.99 |

#### Cosa notiamo?

Le due classifiche sono **completamente diverse**! Solo Daft Punk e James Brown compaiono in entrambe.

1. Daniel Ingram (compositore per My Little Pony) ha **il PageRank più alto in assoluto** (59.85), ma solo 130 campionamenti (non nella top 10 per volume). Come vedremo dopo, è un'anomalia dovuta a una comunità ristretta e densa.

2. James Brown ha "solo" 203 campionamenti (nono per volume), ma è **secondo per autorità** (45.67). Significa che chi lo campiona sono artisti molto influenti.

3. **Denonbu e 外神田文芸高校** (progetti musicali giapponesi legati agli anime) appaiono al 4° e 5° posto per autorità, pur non essendo nella top 10 per volume. Stesso pattern di Daniel Ingram: comunità dense e isolate.

4. **Daft Punk** è primo per volume (370) ma scende al 7° posto per autorità (27.08). Pur essendo i più campionati in assoluto, i loro sampler sono meno influenti.

---

### 4.3 Volume vs Authority — "Gli Influencer Nascosti"

**Figura: `fig1_volume_vs_authority.pdf`** (3 pannelli: volume, autorità, scatter con sorprese evidenziate)

Questo grafico confronta i top 20 artisti in due barre:
- Barra chiara/sinistra: **Volume** (In-Degree) — quante volte sono campionati
- Barra scura/destra: **Authority** (PageRank) — quanto sono influenti strutturalmente

Il grafico rivela tre gruppi di artisti:

#### 1. I "Re del Volume" (tanto campionati, autorità media)
- **Daft Punk** (370 eventi) e **Lady Gaga** (281)
- Sono i più campionati in termini assoluti
- Ma la loro autorità PageRank è più bassa di artisti con meno campionamenti
- Perché? I loro sampler sono tantissimi ma meno influenti

#### 2. Le "Autorità Classiche" (meno campionati, ma da artisti importanti)
- **James Brown**, **Lyn Collins**, **Kraftwerk**
- Hanno meno campionamenti in termini assoluti
- Ma PageRank alto perché chi li campiona sono artisti a loro volta molto influenti
- Esempio: James Brown è campionato da hip-hop leggendari (Public Enemy, ecc.), che trasmettono la loro autorità a lui

#### 3. L'Outlier "Community-Driven": Daniel Ingram
- Ha solo 130 campionamenti (molti meno di Daft Punk)
- Ma **PageRank più alto di tutti** (59.85)
- Perché? La sua comunità di fan/remixer (My Little Pony) forma un "cluster denso": tanti si campionano a vicenda, creando un circuito chiuso di autorità

---



---

### 4.5 Hub Analysis + Bridges — Classificazione e Ponti Musicali

**Figura: `fig2_hub_bridges.pdf`** (2 pannelli: scatter hub + barre bridges, colorati per cluster)

Questa figura fonde due analisi complementari.

#### Pannello sinistro: Hub Analysis (classificazione in 4 quadranti)
Mette in relazione Out-Degree (quanti campionamenti usa) e In-Degree (quante volte è campionato). I puntini sono colorati per cluster di appartenenza. Le linee tratteggiate sono le mediane.

I **4 quadranti**:

1. **Pure Authorities** (in alto a sinistra) — artisti **tanto campionati** ma che **usano pochi campionamenti** (James Brown, Daft Punk, The Beatles). Sono la "materia prima" della musica.

2. **Bridges** (in alto a destra) — artisti che **sia campionano tanto che vengono campionati**. Sono i ponti dell'ecosistema musicale: **JAY-Z**, **Beastie Boys**, **Ye (Kanye West)**.

3. **Heavy Samplers** (in basso a destra) — artisti che **usano molti campionamenti** ma **non vengono quasi mai ricambiati**.

4. **Peripheral Artists** (in basso a sinistra) — la **maggior parte degli artisti**, con pochi campionamenti in entrata e in uscita.

#### Pannello destro: Top 15 Bridges
Mostra gli artisti con il più alto **punteggio ponte** (media geometrica In × Out), con barre colorate per cluster. JAY-Z, Beastie Boys e Ye sono in cima, seguiti da Madonna, Fatboy Slim, David Bowie. Fungono da **connettori evolutivi** cruciali per la diffusione dell'influenza.

---

### 4.7 Validazione Esterna — Quanto è affidabile il PageRank?

Abbiamo confrontato le nostre classifiche PageRank con una **fonte esterna indipendente**: **WhoSampled**, un sito web dedicato ai campionamenti musicali.

#### Come abbiamo fatto
1. Preso la lista di WhoSampled dei **30 artisti più campionati** (ground truth)
2. Calcolato il PageRank per quegli stessi artisti
3. Calcolato il **coefficiente di correlazione di Spearman (ρ)**

#### Il risultato
> **ρ = 0.72** — correlazione **forte positiva** (p < 0.0001)

#### Cosa significa?
0.72 è un risultato molto buono. Significa che le nostre classifiche PageRank sono **fortemente allineate** con la realtà di mercato misurata da WhoSampled, ma catturano anche informazioni aggiuntive che il semplice conteggio non vede.

1. **WhoSampled conta volumi** (quante volte un artista è campionato)
2. **PageRank misura influenza strutturale** (da chi sei campionato, non solo quanto)

La correlazione forte ma non perfetta è il risultato ideale: significa che **il PageRank non è solo una replica del conteggio**, ma aggiunge una dimensione informativa nuova.

#### Esempi delle differenze
| Artista | Rank WhoSampled (ground truth) | Rank PageRank (sistema) |
|---------|-------------------------------|------------------------|
| James Brown | 1° (il più campionato) | **2°** — quasi perfetto |
| Public Enemy | **2°** | **56°** — forte differenza: PE è molto campionato ma i loro sampler sono meno influenti strutturalmente |
| Lyn Collins | **5°** | **6°** — ottimo allineamento |
| Kraftwerk | **6°** | **10°** — ancora nella top 10 |
| Isaac Hayes | **23°** | **20°** — il PageRank lo promuove leggermente |

La correlazione forte (0.72) ci dice che il nostro PageRank:
- **Cattura fedelmente** la realtà del mercato (non è sbagliato)
- **Aggiunge informazioni** che il semplice conteggio non fornisce

---

### 4.8 Authority Context — Perché Daniel Ingram è il numero 1?

**Figura: `fig3_authority_context.pdf`**

Questa figura spiega **l'anomalia Daniel Ingram**: un compositore di My Little Pony con PageRank più alto di James Brown.

#### Cosa mostra il grafico
Ogni barra orizzontale rappresenta un artista. È divisa in due colori:
- **Colore 1**: percentuale di campionamenti che arrivano da DENTRO la stessa comunità (influenza interna)
- **Colore 2**: percentuale di campionamenti che arrivano da FUORI la comunità (influenza esterna)

#### I due casi estremi

**James Brown** (autorità "sana"):
- Quasi tutta la barra è di un colore = influenza **esterna**
- Significa: James Brown è campionato da artisti di TUTTI i generi, in tutto il mondo, attraverso decenni
- È una **autorità globale**: la sua influenza attraversa generi e confini

**Daniel Ingram** (autorità "insulare"):
- Quasi tutta la barra è dell'altro colore = influenza **interna**
- Significa: la stragrande maggioranza dei campionamenti arriva dalla sua comunità (fandom di My Little Pony)
- C'è una rete densa di fan/remixer che si campionano a vicenda, formando un cluster chiuso
- Questo "circuito chiuso" fa volare il PageRank (perché tutti nella comunità si danno autorità a vicenda)

Lo stesso vale per:
- Denonbu e 外神田文芸高校 (progetti musicali giapponesi legati a franchise di anime)
- Sono fenomeni di **internet culture**: comunità molto attive e dense, ma relativamente isolate dal mainstream

#### Perché è importante
Questa analisi rivela **un limite del PageRank**: può essere "ingannato" da comunità dense e isolate. Non tutta l'autorità è uguale — c'è differenza tra:
- Essere influente **globalmente** (James Brown)
- Essere influente **all'interno di una comunità chiusa** (Daniel Ingram)

---

### 4.9 Riepilogo Statistiche di Rete

Tabella riassuntiva dal report:

| Metrica | Valore |
|---------|--------|
| **Eventi di campionamento** | 57.741 |
| **Canzoni uniche** | 67.638 |
| **Artisti unici** | 23.242 |
| Di cui artisti che campionano | 9.670 |
| Di cui artisti campionati | 16.390 |
| **Densità del grafo** | 0.00001262 (molto sparso) |
| **In-Degree medio** | 3.52 |
| **In-Degree max** | 370 (Daft Punk) |
| **Out-Degree medio** | 5.97 |
| **Out-Degree max** | 1.488 |

---

### 4.10 Community Detection — Le Comunità Genealogiche

L'algoritmo Louvain ha rilevato:

#### I numeri
- **1.774 comunità** (famiglie genealogiche)
- **Comunità più grande**: JAY-Z (**2.196 artisti**)
- **Intra-cluster edge fraction**: **0.7169**
- **Dimensione media comunità**: **13.07 artisti**

#### Cosa significa "Intra-cluster edge fraction 0.7169"?

La "frazione di archi intra-cluster" misura quanti archi cadono **dentro** la stessa comunità rispetto al totale. Un valore di 0.7169 significa:
- **~72%** degli archi sono tra artisti della stessa comunità (stessa "famiglia musicale")
- **~28%** degli archi attraversano i confini tra comunità (artisti di famiglie diverse che si campionano)

**Cosa ci dice?**
Che la musica moderna è **molto più strutturata** di quanto emerso con LPA. La maggior parte del campionamento (72%) rimane all'interno della stessa genealogia musicale, mentre solo il 28% attraversa confini comunitari. Questo è più coerente con l'intuizione che gli artisti tendono a campionare dalle loro tradizioni di riferimento.

Rispetto al vecchio algoritmo LPA (che produceva 13.855 cluster con solo 19% intra-cluster), Louvain trova una partizione **più significativa e stabile**, senza frammentare eccessivamente la rete.

#### La figura

**Figura: `fig5_cluster_distribution.pdf`**

Cosa mostra:
- Asse X: dimensione della comunità (quanti artisti contiene)
- Asse Y: quante comunità hanno quella dimensione
- Scala log-log

**Cosa si vede:**
- Una **lunga coda**: tantissime comunità piccole (2-5 artisti) e poche grandi.
- La maggior parte delle comunità sono "catene genealogiche" corte: un artista campiona un altro, che a sua volta ne campiona un altro, ma la catena è breve.
- Poche comunità grandi rappresentano **movimenti musicali estesi** (JAY-Z, SmadaLeinad, DJ Schmolli).

**1.774 comunità è un numero ragionevole?**
Considerando 23.242 artisti, significa che in media ogni comunità contiene circa 13 artisti. È un numero che bilancia granularità e significatività: abbastanza comunità da catturare nicchie e sottogeneri, ma non così tante da polverizzare la rete in micro-cluster irrilevanti.

---

### 4.11 Rete Artistica Colorata per Cluster

**Figura: `fig4_cluster_artist_network.pdf`**

Mostra i top 50 artisti per PageRank, ogni nodo colorato in base al suo cluster Louvain. Questa visualizzazione colma il divario tra l'analisi dei singoli artisti e la struttura astratta delle comunità: rivela quali cluster producono gli artisti strutturalmente più importanti e se l'influenza scorre dentro o attraverso i confini di cluster.

- **Archi solidi** (continui) = campionamento intra-cluster (stessa comunità)
- **Archi tratteggiati** = campionamento inter-cluster (attraverso comunità diverse)
- **Dimensione nodo** = proporzionale al grado di connessione

---

## 5. Discussione

### 5.1 Implicazioni Teoriche

Tre scoperte principali:

#### 1. Autorità ≠ Popolarità
Il PageRank dimostra che la **posizione strutturale** conta quanto (o più) del volume grezzo. Essere campionati da artisti influenti è diverso da essere campionati da sconosciuti. Questo spiega perché:
- **Daniel Ingram** e **Denonbu** (fenomeni internet) hanno autorità altissima nonostante volumi modesti
- **James Brown** rimane un pilastro nonostante non sia primo per volume

#### 2. Genealogia cross-genere
Il fatto che franchise di animazione e videogiochi emergano come autorità dimostra che il campionamento crea **percorsi genealogici inaspettati** che vanno oltre i tradizionali confini di genere musicale.

#### 3. Preferential Attachment (attaccamento preferenziale)
La distribuzione a power-law conferma che l'influenza musicale segue le stesse **leggi matematiche** delle reti di citazioni, delle reti sociali e del web: pochi hub dominano, la maggior parte resta periferica.

### 5.2 Contributi Metodologici

Cosa dimostra questo progetto dal punto di vista tecnico:

1. **Implementazione da zero**: PageRank implementato manualmente in PySpark e Louvain community detection via NetworkX, dimostrando comprensione profonda degli algoritmi distribuiti e di analisi di rete.

2. **Data Engineering**: navigare lo schema complesso di MusicBrainz (Terza Forma Normale) e trasformarlo in un grafo è un lavoro di data engineering reale.

3. **Convergenza ottimizzata**: Schema a **50 iterazioni fisse** con checkpointing per prevenire stack overflow, e corretta redistribuzione della massa dei dangling nodes.

4. **Visualizzazione pulita**: i grafici sono progettati per essere chiari nonostante la complessità (es. mostrare solo connessioni con peso ≥ 2 nella rete top 15).

### 5.3 Limitazioni e Lavoro Futuro

#### Limitazioni

1. **Qualità dei dati**: nonostante la rimozione dei self-loop, MusicBrainz contiene ancora errori: artisti placeholder, doppioni. Un lavoro futuro potrebbe automatizzare la pulizia.

2. **Analisi statica**: il grafo è "fotografato" in un momento. Non abbiamo le date di pubblicazione. Se le avessimo, potremmo vedere come i pattern di campionamento sono **cambiati nei decenni**.

3. **Niente tag di genere**: Louvain non usa etichette di genere. Se avessimo i tag (Hip Hop, Rock, EDM, ecc.), potremmo **validare** se le nostre comunità coincidono con i generi reali.

4. **Single-hop**: analizziamo solo campionamenti diretti (X campiona Y). Potremmo estendere a **multi-hop** (X campiona Y, Y campiona Z → X è nipote di Z).

#### Lavoro futuro
- Aggiungere analisi temporale (come evolve il sampling?)
- Validare i cluster con tag di genere espliciti
- Tracciare genealogie multi-generazionali

---

## 6. Conclusione

### Cosa abbiamo fatto
Abbiamo applicato **tecniche di analisi di rete** per scoprire la genealogia nascosta della musica moderna. Modellando i campionamenti come un grafo diretto e implementando PageRank in Apache Spark e Louvain community detection via NetworkX, abbiamo rivelato **pattern strutturali invisibili** alle metriche basate sul volume.

### Le scoperte principali (Key Findings)

1. **Daniel Ingram** (My Little Pony) e **Denonbu** (progetto musicale giapponese) emergono come le nuove autorità inaspettate. Mostrano l'enorme influenza strutturale delle **fanbase internet dense** sui campionamenti contemporanei.

2. **James Brown** e i classici del funk rimangono pilastri fondamentali, ma ora condividono il vertice con creatori dell'era digitale.

3. **Distribuzione a power-law**: la rete segue la legge di potenza. Il **top 1%** degli artisti controlla **23.5%** di tutti i campionamenti.

4. **1.774 famiglie genealogiche** rilevate, con una frazione intra-cluster di **0.7169** (il 72% degli archi dentro la stessa comunità). Questo significa che la musica moderna è **fortemente strutturata**: la maggior parte del campionamento rimane all'interno della stessa genealogia.

5. **Entity Resolution**: passando agli ID ufficiali MusicBrainz (invece di confrontare nomi per approssimazione), abbiamo scoperto una rete molto più grande (58k+ archi) di quanto stimato precedentemente.

6. **Convergenza stabile**: il PageRank con 50 iterazioni fisse e checkpointing garantisce convergenza senza errori di memoria, grazie alla corretta gestione dei "dangling nodes" e della redistribuzione della massa.

### Il messaggio finale

> Questa analisi sposta la conversazione da **"chi ha venduto più dischi"** a **"chi ha strutturalmente plasmato la musica moderna"**.

Usando teoria dei grafi e calcolo distribuito, abbiamo **mappato il DNA del suono contemporaneo** — dimostrando che l'influenza scorre attraverso **reti**, non classifiche.

### Applicazioni future
Questa metodologia può estendersi ad altri domini:
- **Reti di citazioni accademiche**: quali articoli sono fondamentali?
- **Influenza social**: chi guida le conversazioni?
- **Dipendenze software**: quali librerie stanno sotto lo sviluppo moderno?

Gli strumenti sono universali; le intuizioni, profonde.
