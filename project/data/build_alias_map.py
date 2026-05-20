import json, os, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("BuildAliasMap").config("spark.driver.memory", "4g").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Loading graph...")
df = spark.read.parquet("outputs/music_graph.parquet")
df.cache()

all_names = df.select("Sampler_Artist_Name").union(df.select("Original_Artist_Name")).distinct()
name_rows = all_names.collect()
all_names_list = sorted(set(r[0] for r in name_rows if r[0] and r[0] not in ("[unknown]", "[no artist]", "")))
print(f"Total unique artist names: {len(all_names_list)}")

# ---------------------------------------------------------------------------
# Approach: prefix-match + suffix validation
#
# Step 1 — Find all cases where one artist name is a left-prefix of another
#           and the shorter name is ≥2 tokens (avoid single-word collisions).
# Step 2 — Validate the suffix (everything after the prefix) against known
#           qualifier patterns:
#             a) Starts with a conjunction:  &, and, feat., featuring, ft.,
#                                            +, vs., versus, et, und
#             b) Single-word qualifier:      Trio, Quartet, Quintet, Sextet,
#                                            Quintette, Orchestra, Ensemble,
#                                            Group, Project, Chorus,
#                                            Incorporated, Ubiquity, Band
#             c) Multi-word suffix ending   (e.g. "Elektric Band" ends
#                in a qualifier word:        with "Band")
# ---------------------------------------------------------------------------

SINGLE_WORD_QUALIFIERS = {
    'trio', 'quartet', 'quintet', 'sextet', 'quintette',
    'orchestra', 'ensemble', 'group', 'project', 'chorus',
    'incorporated', 'band', 'ubiquity',
}

CONJUNCTIONS = {'&', 'and', 'feat.', 'featuring', 'ft.', '+', 'vs.', 'versus', 'et', 'und'}

def suffix_is_qualifier(suffix_words):
    """Check whether suffix tokens represent a known qualifier phrase."""
    if not suffix_words:
        return False
    first = suffix_words[0].rstrip('.')
    if first in CONJUNCTIONS:
        return True
    if len(suffix_words) == 1:
        return suffix_words[0] in SINGLE_WORD_QUALIFIERS
    return suffix_words[-1] in SINGLE_WORD_QUALIFIERS

def normalize(n):
    return re.sub(r'\s+', ' ', n.lower().strip())

norm_names = {normalize(n): n for n in all_names_list}

prefix_aliases = {}
for name in all_names_list:
    nk = normalize(name)
    words = nk.split()
    if len(words) < 3:
        continue                     # need at least prefix(2) + suffix(1)
    for i in range(len(words) - 1, 1, -1):          # longest prefix first
        prefix_key = ' '.join(words[:i])
        suffix_words = words[i:]
        if prefix_key not in norm_names:
            continue
        canonical = norm_names[prefix_key]
        if canonical.lower() == name.lower():
            continue                 # case variant → handled separately
        if suffix_is_qualifier(suffix_words):
            prefix_aliases[name] = canonical
            break

print(f"\nPrefix-match + suffix-filter aliases found: {len(prefix_aliases)}")

# ---------------------------------------------------------------------------
# Case variants (JADE vs Jade vs jade)
# ---------------------------------------------------------------------------
case_aliases = {}
from collections import defaultdict
by_lower = defaultdict(list)
for n in all_names_list:
    by_lower[n.lower().strip()].append(n)

for key, names in by_lower.items():
    if len(names) > 1:
        # Pick the best-cased name as canonical
        canonical = max(names, key=lambda x: (x[0].isupper(), x[0].isalpha(), -len(x)))
        for n in names:
            if n != canonical:
                case_aliases[n] = canonical

print(f"Case-variant aliases found: {len(case_aliases)}")

# ---------------------------------------------------------------------------
# Merge: prefix aliases override case variants (they are more specific)
# ---------------------------------------------------------------------------
alias_map = {}
alias_map.update(case_aliases)
alias_map.update(prefix_aliases)

print(f"\nTotal alias entries: {len(alias_map)}")

# Save
os.makedirs("data", exist_ok=True)
with open("data/artist_aliases.json", "w", encoding="utf-8") as f:
    json.dump(alias_map, f, indent=2, ensure_ascii=False)

print("\n--- ALIAS MAP ---")
for variant, canonical in sorted(alias_map.items(), key=lambda x: x[1]):
    print(f'  "{variant}" → "{canonical}"')

spark.stop()
