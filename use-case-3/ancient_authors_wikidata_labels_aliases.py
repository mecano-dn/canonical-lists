import csv
import time
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions

# === CONFIG ===
ENDPOINT_URL = "https://query.wikidata.org/sparql"
INPUT_FILE = "ancient_authors_wikidata_with_precision.csv"  # must have 'wikidata_id' column
OUTPUT_FILE = "ancient_authors_labels_aliases_by_lang.csv"
BATCH_SIZE = 200
TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
THROTTLE_SECONDS = 0.2  # be nice to WDQS

# === SPARQL SETUP ===
sparql = SPARQLWrapper(ENDPOINT_URL)
sparql.setReturnFormat(JSON)
sparql.setTimeout(TIMEOUT_SECONDS)

def run_query_with_backoff(query, max_retries=MAX_RETRIES):
    retry = 0
    delay = 5
    while retry <= max_retries:
        try:
            sparql.setQuery(query)
            return sparql.query().convert()
        except (SPARQLExceptions.EndPointInternalError,
                SPARQLExceptions.QueryBadFormed,
                Exception) as e:
            retry += 1
            if retry > max_retries:
                raise
            print(f"⚠️  Query failed (attempt {retry}/{max_retries}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2

def read_qids_from_csv(filename):
    qids = []
    with open(filename, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            qid = (row.get("wikidata_id") or "").strip()
            if not qid:
                continue
            if qid.startswith("http"):
                qid = qid.rsplit("/", 1)[-1]
            qids.append(qid)
    return qids

def build_labels_aliases_query(qids):
    """
    Reliable pattern:
      - Subselect 1: labels per (author, lang_code) with SAMPLE(label)
      - Subselect 2: aliases per (author, lang_code) with GROUP_CONCAT
      - UNION them and aggregate again so each (author, lang_code) gets both fields
    """
    values = " ".join(f"wd:{qid}" for qid in qids)
    return f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX wd:   <http://www.wikidata.org/entity/>

SELECT ?author ?lang_code
       (SAMPLE(?_label) AS ?label)
       (SAMPLE(?_aliases) AS ?aliases)
WHERE {{
  {{
    SELECT ?author ?lang_code (SAMPLE(?label) AS ?_label)
    WHERE {{
      VALUES ?author {{ {values} }}
      ?author rdfs:label ?label .
      BIND(LANG(?label) AS ?lang_code)
    }}
    GROUP BY ?author ?lang_code
  }}
  UNION
  {{
    SELECT ?author ?lang_code (GROUP_CONCAT(DISTINCT ?alias;separator="|") AS ?_aliases)
    WHERE {{
      VALUES ?author {{ {values} }}
      ?author skos:altLabel ?alias .
      BIND(LANG(?alias) AS ?lang_code)
    }}
    GROUP BY ?author ?lang_code
  }}
}}
GROUP BY ?author ?lang_code
"""

def write_csv(rows, filename):
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["wikidata_id", "lang_code", "label", "aliases"])
        writer.writerows(rows)

def main():
    print(f"📥 Reading Q-IDs from: {INPUT_FILE}")
    qids = read_qids_from_csv(INPUT_FILE)
    print(f"🔢 Found {len(qids)} Q-IDs.")

    all_rows = []

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i+BATCH_SIZE]
        print(f"🔍 Batch {i//BATCH_SIZE + 1}: {len(batch)} IDs")

        query = build_labels_aliases_query(batch)
        results = run_query_with_backoff(query)

        for b in results["results"]["bindings"]:
            author_uri = b["author"]["value"]
            qid = author_uri.rsplit("/", 1)[-1]
            lang_code = b.get("lang_code", {}).get("value", "")
            label = b.get("label", {}).get("value", "")
            aliases = b.get("aliases", {}).get("value", "")

            all_rows.append([qid, lang_code, label, aliases])

        time.sleep(THROTTLE_SECONDS)

    print(f"💾 Writing {len(all_rows)} rows to {OUTPUT_FILE}")
    write_csv(all_rows, OUTPUT_FILE)
    print("✅ Done.")

if __name__ == "__main__":
    main()
