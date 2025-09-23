import csv
import time
from collections import defaultdict
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions

# === CONFIG ===
ENDPOINT_URL = "https://query.wikidata.org/sparql"
INPUT_FILE = "ancient_authors_wikidata_with_precision.csv"
OUTPUT_FILE = "ancient_authors_ids.csv"
BATCH_SIZE = 100
ID_PROPS = [
    ("P11252", "trismegistos_id"),
    ("P7041", "perseus_id"),
    ("P12869", "lagl_id"),
    ("P11790", "chap_id"),
    ("P7168", "fgrhist_id"),
    ("P3576", "tlg_id"),
    ("P6831", "pinakes_id"),
    ("P6862", "digliblt_id"),
    ("P6941", "phi_id"),
    ("P6999", "mda_id"),
    ("P7038", "dco_id"),
    ("P7042", "lla_id"),
    ("P7908", "clavis_id"),
    ("P7935", "cca_id"),
    ("P8065", "ciris_id"),
    ("P8122", "dll_id"),
    ("P8163", "dk_id"),
    ("P10536", "rspa_id"),
]

HEADERS = ["wikidata_id"] + [name for _, name in ID_PROPS]

# === SPARQL SETUP ===
sparql = SPARQLWrapper(ENDPOINT_URL)
sparql.setReturnFormat(JSON)
sparql.setTimeout(60)

def run_query_with_backoff(query, max_retries=5):
    retry_count = 0
    delay = 5

    while retry_count <= max_retries:
        try:
            sparql.setQuery(query)
            return sparql.query().convert()
        except (SPARQLExceptions.EndPointInternalError, SPARQLExceptions.QueryBadFormed, Exception) as e:
            print(f"⚠️ Query failed (attempt {retry_count + 1}): {e}")
            time.sleep(delay)
            delay *= 2
            retry_count += 1
    raise RuntimeError("🛑 Query failed after max retries.")

def build_id_lookup_query(wikidata_ids):
    values = " ".join(f"wd:{qid}" for qid in wikidata_ids)
    prop_clauses = "\n".join(
        f"OPTIONAL {{ ?author wdt:{pid} ?{varname} . }}" for pid, varname in ID_PROPS
    )
    select_vars = " ".join(f"?{varname}" for _, varname in ID_PROPS)

    return f"""
    SELECT ?author {select_vars} WHERE {{
      VALUES ?author {{ {values} }}
      {prop_clauses}
    }}
    """

def read_wikidata_ids_from_csv(filename):
    with open(filename, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        return [row["wikidata_id"] for row in reader if row.get("wikidata_id")]

def write_results_to_csv(data, filename):
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(HEADERS)
        writer.writerows(data)

def main():
    print("📥 Reading Wikidata IDs from input CSV...")
    wikidata_ids = read_wikidata_ids_from_csv(INPUT_FILE)
    print(f"🔢 Found {len(wikidata_ids)} IDs.")

    merged = defaultdict(dict)

    for i in range(0, len(wikidata_ids), BATCH_SIZE):
        batch = wikidata_ids[i:i + BATCH_SIZE]
        print(f"🔍 Processing batch {i // BATCH_SIZE + 1} ({len(batch)} IDs)...")

        query = build_id_lookup_query(batch)
        results = run_query_with_backoff(query)

        for result in results["results"]["bindings"]:
            qid = result["author"]["value"].split("/")[-1]
            record = merged[qid]
            record["wikidata_id"] = qid

            for _, name in ID_PROPS:
                record[name] = result.get(name, {}).get("value", "")

    print(f"💾 Writing results to {OUTPUT_FILE}...")
    rows = [[record.get(h, "") for h in HEADERS] for record in merged.values()]
    write_results_to_csv(rows, OUTPUT_FILE)
    print("✅ Done.")

if __name__ == "__main__":
    main()
