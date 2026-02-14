# MusicBrainz Data Directory

⚠️ **This directory is excluded from Git** due to file size (~500MB) and licensing.

## 📥 How to Obtain the Data

### Option 1: Download from MusicBrainz (Recommended)

1. **Visit the MusicBrainz database download page:**
   ```
   https://musicbrainz.org/doc/MusicBrainz_Database/Download
   ```

2. **Download the latest PostgreSQL dump:**
   ```bash
   # Download (may take 5-10 minutes)
   wget http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST/mbdump.tar.bz2
   
   # Alternative: European mirror (faster from Italy)
   wget https://eu.ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST/mbdump.tar.bz2
   ```

3. **Extract only the required tables:**
   ```bash
   tar -xjf mbdump.tar.bz2
   cd mbdump/mbdump/
   
   # Copy required files to this directory
   cp artist artist_credit artist_credit_name link link_type l_recording_recording recording \
      /home/jacopo/Documenti/Trento/DataMining/MusicProject/dataset/mbdump/
   
   # Clean up
   cd ../..
   rm -rf mbdump mbdump.tar.bz2
   ```

### Option 2: Use Provided Sample Dataset

If your professor provided a sample dataset for this course:

```bash
# Extract the provided archive
tar -xzf musicbrainz_sample.tar.gz -C /path/to/dataset/mbdump/
```

## 📂 Required Files

After setup, this directory **must contain** these 7 files:

| File | Description | Approx. Size | Format |
|------|-------------|--------------|--------|
| `artist` | Artist metadata | ~150 MB | TSV (tab-separated) |
| `artist_credit` | Credit information | ~50 MB | TSV |
| `artist_credit_name` | Artist-credit mapping | ~80 MB | TSV |
| `recording` | Song/track information | ~200 MB | TSV |
| `link` | Generic relationship links | ~30 MB | TSV |
| `link_type` | Relationship type definitions | ~1 MB | TSV |
| `l_recording_recording` | **Recording-to-recording links** | ~20 MB | TSV |

**Total size: ~531 MB**

⚠️ **Note**: All files are **tab-separated values WITHOUT headers**. Schema definitions are hardcoded in `../src/data_preparation.py`.

## 🎯 Critical File: `l_recording_recording`

This file contains the **sampling relationships**. We filter for:
- **Link Type ID 69**: "samples material" (direct sampling)
- **Link Type ID 231**: "samples" (general sampling)

Example row (tab-separated):
```
12345	69	98765	87654	...
  ↑      ↑     ↑      ↑
 ID   Type  Song1  Song2
```

## ✅ Verification

After placing the files, verify the setup:

```bash
cd ..  # Go to dataset/ directory
python3 -c "
import os
required = ['artist', 'artist_credit', 'artist_credit_name', 'link', 'link_type', 'l_recording_recording', 'recording']
missing = [f for f in required if not os.path.exists(f'mbdump/{f}')]
if missing:
    print(f'❌ Missing files: {missing}')
    exit(1)
else:
    print('✅ All required files present!')
    total_size = 0
    for f in required:
        size = os.path.getsize(f'mbdump/{f}') / (1024*1024)
        total_size += size
        print(f'  {f}: {size:.1f} MB')
    print(f'\\nTotal size: {total_size:.1f} MB')
"
```

Expected output:
```
✅ All required files present!
  artist: 153.2 MB
  artist_credit: 48.7 MB
  artist_credit_name: 79.3 MB
  link: 28.4 MB
  link_type: 0.8 MB
  l_recording_recording: 19.6 MB
  recording: 201.3 MB

Total size: 531.1 MB
```

## 🔄 Test Data Processing

Run the data preparation script to verify:

```bash
cd ..
source venv/bin/activate
python3 src/data_preparation.py
```

Expected console output:
```
Caricamento tabelle...
  - artist: 2,168,943 rows
  - recording: 31,876,294 rows
  - l_recording_recording: 501,287 rows

Filtraggio relazioni...
  - Sampling relationships (69, 231): 22,135 edges

Total edges before self-loop removal: 22135
Edges after removing self-loops: 20728
Self-loops removed: 1407 edges (6.4%)

✓ Graph saved to: outputs/music_graph.parquet
```

## 📄 Data License

**MusicBrainz data is licensed under CC BY-NC-SA 3.0:**
- ✅ Attribution required
- ✅ Share-alike (derivative works under same license)
- ❌ **Non-commercial use only**

**Proper citation:**
```
MusicBrainz Database. (2025). MusicBrainz Foundation.
https://musicbrainz.org/doc/MusicBrainz_Database
Licensed under CC BY-NC-SA 3.0
```

For more information: https://musicbrainz.org/doc/About/Data_License

## 🆘 Troubleshooting

### Download issues

If download fails or is slow:
```bash
# Resume interrupted download
wget -c http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST/mbdump.tar.bz2

# Or use aria2 for faster parallel downloads
aria2c -x 16 http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST/mbdump.tar.bz2
```

### File encoding issues

If you get encoding errors during processing:
```bash
# Check file encoding
file artist
# Should be: ASCII text or UTF-8 Unicode text

# If wrong encoding, convert:
iconv -f ISO-8859-1 -t UTF-8 artist > artist_utf8
mv artist_utf8 artist
```

### Schema mismatch

If the schema has changed in newer dumps:
```bash
# Check first line to see column structure
head -n 1 artist
head -n 1 recording

# Compare with expected schema in src/data_preparation.py (lines 15-60)
```

## 🔗 Useful Links

- **MusicBrainz Database Docs**: https://musicbrainz.org/doc/MusicBrainz_Database
- **Download Page**: https://musicbrainz.org/doc/MusicBrainz_Database/Download
- **Schema Documentation**: https://musicbrainz.org/doc/MusicBrainz_Database/Schema
- **FTP Mirror**: http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/

---

**Need help?** See the main README at `../../README.md` or consult course documentation.
