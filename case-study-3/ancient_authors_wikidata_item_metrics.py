import csv
import time
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions

# === CONFIG ===
ENDPOINT_URL = "https://query.wikidata.org/sparql"
INPUT_FILE = "ancient_authors_wikidata_with_precision.csv"  # must have 'wikidata_id' column
OUTPUT_FILE = "ancient_authors_item_metrics_with_projects.csv"
BATCH_SIZE = 50
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

def build_metrics_query(qids):
    values = " ".join(f"wd:{qid}" for qid in qids)
    return f"""
PREFIX schema:  <http://schema.org/>
PREFIX wdt:     <http://www.wikidata.org/prop/direct/>
PREFIX wd:      <http://www.wikidata.org/entity/>
PREFIX wikibase:<http://wikiba.se/ontology#>

SELECT ?item
       (SAMPLE(?statements_)  AS ?statements)
       (SAMPLE(?identifiers_) AS ?identifiers)
       (SAMPLE(?sitelinks_)   AS ?total_sitelinks)

       # Per-project counts via SUM of booleans (no UNDEF)
       (SUM(IF(?project="wikipedia",   1, 0)) AS ?wikipedia_count)
       (SUM(IF(?project="wiktionary",  1, 0)) AS ?wiktionary_count)
       (SUM(IF(?project="wikiquote",   1, 0)) AS ?wikiquote_count)
       (SUM(IF(?project="wikisource",  1, 0)) AS ?wikisource_count)
       (SUM(IF(?project="wikibooks",   1, 0)) AS ?wikibooks_count)
       (SUM(IF(?project="wikinews",    1, 0)) AS ?wikinews_count)
       (SUM(IF(?project="wikiversity", 1, 0)) AS ?wikiversity_count)
       (SUM(IF(?project="wikivoyage",  1, 0)) AS ?wikivoyage_count)
       (SUM(IF(?project="commons",     1, 0)) AS ?commons_count)
       (SUM(IF(?project="meta",        1, 0)) AS ?meta_count)
       (SUM(IF(?project="wikispecies", 1, 0)) AS ?wikispecies_count)
       (SUM(IF(?project="wikidata",    1, 0)) AS ?wikidata_count)
       (SUM(IF(?project="incubator",   1, 0)) AS ?incubator_count)
       (SUM(IF(?project="wikifunctions",1,0)) AS ?wikifunctions_count)

       # Language code lists per project
       (GROUP_CONCAT(DISTINCT IF(?project="wikipedia",   ?lang_code, ""); separator=",") AS ?wikipedia_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wiktionary",  ?lang_code, ""); separator=",") AS ?wiktionary_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikiquote",   ?lang_code, ""); separator=",") AS ?wikiquote_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikisource",  ?lang_code, ""); separator=",") AS ?wikisource_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikibooks",   ?lang_code, ""); separator=",") AS ?wikibooks_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikinews",    ?lang_code, ""); separator=",") AS ?wikinews_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikiversity", ?lang_code, ""); separator=",") AS ?wikiversity_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikivoyage",  ?lang_code, ""); separator=",") AS ?wikivoyage_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="commons",     ?lang_code, ""); separator=",") AS ?commons_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="meta",        ?lang_code, ""); separator=",") AS ?meta_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikispecies", ?lang_code, ""); separator=",") AS ?wikispecies_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikidata",    ?lang_code, ""); separator=",") AS ?wikidata_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="incubator",   ?lang_code, ""); separator=",") AS ?incubator_langs)
       (GROUP_CONCAT(DISTINCT IF(?project="wikifunctions",?lang_code,""); separator=",") AS ?wikifunctions_langs)

WHERE {{
  VALUES ?item {{ {values} }}

  # ✅ correct places to get counts from
  OPTIONAL {{ ?item wikibase:statements  ?statements_  . }}
  OPTIONAL {{ ?item wikibase:identifiers ?identifiers_ . }}
  OPTIONAL {{ ?item wikibase:sitelinks   ?sitelinks_   . }}

  # Sitelinks for per-project breakdown
  OPTIONAL {{
    ?sitelink schema:about ?item ;
              schema:isPartOf ?site .
    BIND( LCASE( STRBEFORE( STRAFTER(STR(?site), "//"), "." ) ) AS ?lang_code )
    BIND(STR(?site) AS ?site_str)
    BIND(
      IF(CONTAINS(?site_str,".wikipedia.org"),   "wikipedia",
      IF(CONTAINS(?site_str,".wiktionary.org"),  "wiktionary",
      IF(CONTAINS(?site_str,".wikiquote.org"),   "wikiquote",
      IF(CONTAINS(?site_str,".wikisource.org"),  "wikisource",
      IF(CONTAINS(?site_str,".wikibooks.org"),   "wikibooks",
      IF(CONTAINS(?site_str,".wikinews.org"),    "wikinews",
      IF(CONTAINS(?site_str,".wikiversity.org"), "wikiversity",
      IF(CONTAINS(?site_str,".wikivoyage.org"),  "wikivoyage",
      IF(CONTAINS(?site_str,"commons.wikimedia.org"), "commons",
      IF(CONTAINS(?site_str,"meta.wikimedia.org"),    "meta",
      IF(CONTAINS(?site_str,"species.wikimedia.org"), "wikispecies",
      IF(CONTAINS(?site_str,".wikidata.org"),         "wikidata",
      IF(CONTAINS(?site_str,"incubator.wikimedia.org"), "incubator",
      IF(CONTAINS(?site_str,".wikifunctions.org"),     "wikifunctions", "other"
      )))))))))))))) AS ?project)
  }}
}}
GROUP BY ?item
"""

def write_csv(rows, filename):
    headers = [
        "wikidata_id", "statements", "identifiers", "total_sitelinks",
        "wikipedia_count", "wiktionary_count", "wikiquote_count", "wikisource_count",
        "wikibooks_count", "wikinews_count", "wikiversity_count", "wikivoyage_count",
        "commons_count", "meta_count", "wikispecies_count", "wikidata_count",
        "incubator_count", "wikifunctions_count",
        "wikipedia_langs", "wiktionary_langs", "wikiquote_langs", "wikisource_langs",
        "wikibooks_langs", "wikinews_langs", "wikiversity_langs", "wikivoyage_langs",
        "commons_langs", "meta_langs", "wikispecies_langs", "wikidata_langs",
        "incubator_langs", "wikifunctions_langs",
    ]
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(headers)
        writer.writerows(rows)

def main():
    print(f"📥 Reading Q-IDs from: {INPUT_FILE}")
    qids = read_qids_from_csv(INPUT_FILE)
    print(f"🔢 Found {len(qids)} Q-IDs.")

    all_rows = []

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i+BATCH_SIZE]
        print(f"🔍 Batch {i//BATCH_SIZE + 1}: {len(batch)} IDs")

        query = build_metrics_query(batch)
        results = run_query_with_backoff(query)

        for b in results["results"]["bindings"]:
            item_uri = b["item"]["value"]
            qid = item_uri.rsplit("/", 1)[-1]

            def gv(k, default=""):
                return b.get(k, {}).get("value", default)

            row = [
                qid,
                gv("statements"), gv("identifiers"), gv("total_sitelinks", "0"),
                gv("wikipedia_count", "0"), gv("wiktionary_count", "0"), gv("wikiquote_count", "0"),
                gv("wikisource_count", "0"), gv("wikibooks_count", "0"), gv("wikinews_count", "0"),
                gv("wikiversity_count", "0"), gv("wikivoyage_count", "0"), gv("commons_count", "0"),
                gv("meta_count", "0"), gv("wikispecies_count", "0"), gv("wikidata_count", "0"),
                gv("incubator_count", "0"), gv("wikifunctions_count", "0"),
                gv("wikipedia_langs"), gv("wiktionary_langs"), gv("wikiquote_langs"),
                gv("wikisource_langs"), gv("wikibooks_langs"), gv("wikinews_langs"),
                gv("wikiversity_langs"), gv("wikivoyage_langs"), gv("commons_langs"),
                gv("meta_langs"), gv("wikispecies_langs"), gv("wikidata_langs"),
                gv("incubator_langs"), gv("wikifunctions_langs"),
            ]
            all_rows.append(row)

        time.sleep(THROTTLE_SECONDS)

    print(f"💾 Writing {len(all_rows)} rows to {OUTPUT_FILE}")
    write_csv(all_rows, OUTPUT_FILE)
    print("✅ Done.")

if __name__ == "__main__":
    main()
