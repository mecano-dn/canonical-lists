import csv
import time
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions
import sys

ENDPOINT_URL = "https://query.wikidata.org/sparql"
OUTPUT_FILE = "ancient_authors_wikidata_with_precision.csv"
HEADERS = [
    "wikidata_id", "name_en", "viaf_id", "bnf_id",
    "birth_year", "birth_precision",
    "death_year", "death_precision",
    "floruit_year", "floruit_precision"
]

# Initialize SPARQL endpoint
sparql = SPARQLWrapper(ENDPOINT_URL)
sparql.setReturnFormat(JSON)
sparql.setTimeout(60)

# Revised SPARQL query with date precision
QUERY = """
SELECT DISTINCT ?author
       (SAMPLE(?name_en) AS ?name_en)
       (GROUP_CONCAT(DISTINCT ?viaf;separator=",") AS ?viaf_ids)
       (SAMPLE(?bnf) AS ?bnf)
       (SAMPLE(?birth_year) AS ?birth_year)
       (SAMPLE(?birth_precision) AS ?birth_precision)
       (SAMPLE(?death_year) AS ?death_year)
       (SAMPLE(?death_precision) AS ?death_precision)
       (SAMPLE(?floruit_year) AS ?floruit_year)
       (SAMPLE(?floruit_precision) AS ?floruit_precision)
WHERE {
  {
    ?author wdt:P11252 ?trismegistos_id .
  } UNION {
    ?author wdt:P7041 ?perseus_id .
  } UNION {
    ?author wdt:P12869 ?lagl_id .
  } UNION {
    ?author wdt:P11790 ?chap_id .
  } UNION {
    ?author wdt:P7168 ?fgrhist_id .
  } UNION {
    ?author wdt:P3576 ?tlg_id .
  } UNION {
    ?author wdt:P6831 ?pinakes_id .
  } UNION {
    ?author wdt:P6862 ?diglibLT_id .
  } UNION {
    ?author wdt:P6941 ?phi_id .
  } UNION {
    ?author wdt:P6999 ?mda_id .
  } UNION {
    ?author wdt:P7038 ?dco_id .
  } UNION {
    ?author wdt:P7042 ?lla_id .
  } UNION {
    ?author wdt:P7908 ?clavis_id ;
            wdt:P31 wd:Q5 .
  } UNION {
    ?author wdt:P7935 ?cca_id .
  } UNION {
    ?author wdt:P8065 ?ciris_id .
  } UNION {
    ?author wdt:P8122 ?dll_id .
  } UNION {
    ?author wdt:P8163 ?dk_id .
  } UNION {
    ?author wdt:P10536 ?rspa_id .
  }

  OPTIONAL { ?author wdt:P214 ?viaf . }
  OPTIONAL { ?author wdt:P268 ?bnf . }
  OPTIONAL { ?author rdfs:label ?name_en . FILTER (lang(?name_en) = "en") }

  OPTIONAL {
    ?author p:P569/psv:P569 ?dob_value .
    ?dob_value wikibase:timeValue ?dob ;
               wikibase:timePrecision ?birth_precision_raw .
    BIND(YEAR(?dob) AS ?birth_year)
    BIND(STR(?birth_precision_raw) AS ?birth_precision)
  }

  OPTIONAL {
    ?author p:P570/psv:P570 ?dod_value .
    ?dod_value wikibase:timeValue ?dod ;
               wikibase:timePrecision ?death_precision_raw .
    BIND(YEAR(?dod) AS ?death_year)
    BIND(STR(?death_precision_raw) AS ?death_precision)
  }

  OPTIONAL {
    ?author p:P1317/psv:P1317 ?floruit_value .
    ?floruit_value wikibase:timeValue ?floruit ;
                   wikibase:timePrecision ?floruit_precision_raw .
    BIND(YEAR(?floruit) AS ?floruit_year)
    BIND(STR(?floruit_precision_raw) AS ?floruit_precision)
  }
}
GROUP BY ?author
LIMIT 10000
"""

def run_query_with_backoff(query, max_retries=5):
    retry_count = 0
    delay = 5

    while retry_count <= max_retries:
        try:
            sparql.setQuery(query)
            return sparql.query().convert()
        except (SPARQLExceptions.EndPointInternalError, SPARQLExceptions.QueryBadFormed, Exception) as e:
            retry_count += 1
            print(f"⚠️  Query failed (attempt {retry_count}/{max_retries}). Retrying in {delay} seconds...", file=sys.stderr)
            time.sleep(delay)
            delay *= 2

    raise RuntimeError("🛑 Query failed after maximum retries.")

def process_results(results):
    processed = []
    seen_ids = set()

    for result in results["results"]["bindings"]:
        wikidata_id = result["author"]["value"].split("/")[-1]
        if wikidata_id in seen_ids:
            continue
        seen_ids.add(wikidata_id)

        name_en = result.get("name_en", {}).get("value", "")
        if name_en.lower().startswith("anonymous") or name_en.lower().startswith("author of") or name_en.lower().startswith("authors of"):
            continue

        viaf = result.get("viaf_ids", {}).get("value", "")
        bnf = result.get("bnf", {}).get("value", "")
        birth_year = result.get("birth_year", {}).get("value", "")
        birth_precision = result.get("birth_precision", {}).get("value", "")
        death_year = result.get("death_year", {}).get("value", "")
        death_precision = result.get("death_precision", {}).get("value", "")
        floruit_year = result.get("floruit_year", {}).get("value", "")
        floruit_precision = result.get("floruit_precision", {}).get("value", "")

        # Time filters
        if floruit_year and floruit_year.isdigit() and int(floruit_year) > 500:
            continue
        if birth_year and birth_year.isdigit() and int(birth_year) > 500:
            continue
        if death_year and death_year.isdigit() and int(death_year) > 500:
            continue

        processed.append([
            wikidata_id,
            name_en,
            viaf,
            bnf,
            birth_year,
            birth_precision,
            death_year,
            death_precision,
            floruit_year,
            floruit_precision
        ])

    return processed

def write_csv(data, filename):
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(HEADERS)
        writer.writerows(data)

def main():
    print("🚀 Starting Wikidata SPARQL query for ancient authors (with precision)...")
    results = run_query_with_backoff(QUERY)
    print("✅ Query successful. Processing results...")

    data = process_results(results)
    print(f"📦 Retrieved {len(data)} authors with VIAF or BnF ID and valid date filtering.")

    write_csv(data, OUTPUT_FILE)
    print(f"💾 Data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
