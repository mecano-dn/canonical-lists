import csv
import time
from collections import defaultdict
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions, POST

# === CONFIG ===
ENDPOINT_URL = "https://query.wikidata.org/sparql"
INPUT_FILE = "ancient_authors_wikidata_with_precision.csv"   # must contain 'wikidata_id'
OUTPUT_FILE = "ancient_authors_languages_names.csv"
BATCH_SIZE_AUTHORS = 50
BATCH_SIZE_LANGS = 200
TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
THROTTLE_SECONDS = 0.2
INCLUDE_WORK_LANGS = True  # set False to skip P50→(P407|P364)

# === SPARQL SETUP ===
sparql = SPARQLWrapper(ENDPOINT_URL)
sparql.setReturnFormat(JSON)
sparql.setTimeout(TIMEOUT_SECONDS)
sparql.setMethod(POST)  # avoid 431 errors with bigger batches
sparql.addCustomHttpHeader("User-Agent", "AncientAuthorsBot/1.0 (contact@example.com)")

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
            delay = min(delay * 2, 60)

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

def build_language_ids_query(qids, include_work_langs=True):
    values = " ".join(f"wd:{qid}" for qid in qids)

    if include_work_langs:
        work_block = """
  OPTIONAL {
    ?work wdt:P50 ?item .
    { ?work wdt:P407 ?lang_work . }
    UNION
    { ?work wdt:P364 ?lang_work . }
  }
  BIND(STRAFTER(STR(?lang_work), "/entity/") AS ?work_lang_qid)
"""
        work_select = '(GROUP_CONCAT(DISTINCT ?work_lang_qid; separator=",") AS ?languages_of_works_ids)'
    else:
        work_block = ""
        work_select = '("" AS ?languages_of_works_ids)'

    return (
        "PREFIX wd:   <http://www.wikidata.org/entity/>\n"
        "PREFIX wdt:  <http://www.wikidata.org/prop/direct/>\n"
        "PREFIX bd:   <http://www.bigdata.com/rdf#>\n\n"
        "SELECT ?item\n"
        "       (GROUP_CONCAT(DISTINCT ?p1412_qid;  separator=\",\") AS ?spoken_written_language_ids)\n"
        "       (GROUP_CONCAT(DISTINCT ?p6886_qid;  separator=\",\") AS ?writing_language_ids)\n"
        "       (GROUP_CONCAT(DISTINCT ?p103_qid;   separator=\",\") AS ?native_language_ids)\n"
        f"       {work_select}\n"
        "WHERE {\n"
        f"  VALUES ?item {{ {values} }}\n\n"
        "  OPTIONAL { ?item wdt:P1412 ?lang1412 . }\n"
        "  OPTIONAL { ?item wdt:P6886 ?lang6886 . }\n"
        "  OPTIONAL { ?item wdt:P103  ?lang103  . }\n\n"
        "  BIND(STRAFTER(STR(?lang1412), \"/entity/\") AS ?p1412_qid)\n"
        "  BIND(STRAFTER(STR(?lang6886), \"/entity/\") AS ?p6886_qid)\n"
        "  BIND(STRAFTER(STR(?lang103),  \"/entity/\") AS ?p103_qid)\n\n"
        f"{work_block}"
        "}\n"
        "GROUP BY ?item\n"
    )

def build_labels_query(lang_qids):
    """Map language Q-IDs to their English rdfs:label."""
    values = " ".join(f"wd:{qid}" for qid in lang_qids)
    return (
        "PREFIX wd:   <http://www.wikidata.org/entity/>\n"
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
        "SELECT ?lang ?label_en WHERE {\n"
        f"  VALUES ?lang {{ {values} }}\n"
        "  ?lang rdfs:label ?label_en .\n"
        "  FILTER(LANG(?label_en) = \"en\")\n"
        "}\n"
    )

def split_dedup(csv_str, sep=","):
    if not csv_str:
        return []
    out = []
    seen = set()
    for part in csv_str.split(sep):
        p = part.strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out

def write_csv(rows, filename):
    headers = [
        "wikidata_id",
        "spoken_written_language_names",
        "writing_language_names",
        "native_language_names",
        "languages_of_works_names",
        "all_languages_names",
    ]
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(headers)
        writer.writerows(rows)

def main():
    # --- Step A: fetch language QIDs per author ---
    qids = read_qids_from_csv(INPUT_FILE)
    print(f"🔢 Found {len(qids)} author Q-IDs.")
    per_author_ids = {}

    for i in range(0, len(qids), BATCH_SIZE_AUTHORS):
        batch = qids[i:i + BATCH_SIZE_AUTHORS]
        print(f"🔍 Languages step A — batch {i//BATCH_SIZE_AUTHORS + 1}: {len(batch)} authors")
        q = build_language_ids_query(batch, include_work_langs=INCLUDE_WORK_LANGS)
        res = run_query_with_backoff(q)

        for b in res["results"]["bindings"]:
            item_uri = b["item"]["value"]
            author_qid = item_uri.rsplit("/", 1)[-1]

            def gv(k): return b.get(k, {}).get("value", "")

            per_author_ids[author_qid] = {
                "spoken_written_ids": gv("spoken_written_language_ids"),
                "writing_ids":        gv("writing_language_ids"),
                "native_ids":         gv("native_language_ids"),
                "works_ids":          gv("languages_of_works_ids"),
            }
        time.sleep(THROTTLE_SECONDS)

    # --- Step B: map all language QIDs to English labels ---
    all_lang_ids = set()
    for v in per_author_ids.values():
        for key in ("spoken_written_ids", "writing_ids", "native_ids", "works_ids"):
            all_lang_ids.update(split_dedup(v.get(key, ""), ","))

    all_lang_ids.discard("")
    print(f"🗂️ Unique language Q-IDs to label: {len(all_lang_ids)}")

    lang_label = {}
    lang_ids_list = list(all_lang_ids)
    for i in range(0, len(lang_ids_list), BATCH_SIZE_LANGS):
        batch = lang_ids_list[i:i + BATCH_SIZE_LANGS]
        print(f"🔠 Labels step B — batch {i//BATCH_SIZE_LANGS + 1}: {len(batch)} language items")
        q = build_labels_query(batch)
        res = run_query_with_backoff(q)
        for b in res["results"]["bindings"]:
            lang_uri = b["lang"]["value"]
            lqid = lang_uri.rsplit("/", 1)[-1]
            label = b.get("label_en", {}).get("value", "")
            if label:
                lang_label[lqid] = label
        time.sleep(THROTTLE_SECONDS)

    # --- Compose final CSV (names only) ---
    rows = []
    for author_qid in qids:  # keep original order where possible
        v = per_author_ids.get(author_qid, {})
        sw_ids = split_dedup(v.get("spoken_written_ids", ""), ",")
        wr_ids = split_dedup(v.get("writing_ids", ""), ",")
        nv_ids = split_dedup(v.get("native_ids", ""), ",")
        wk_ids = split_dedup(v.get("works_ids", ""), ",")

        def map_names(id_list):
            return [lang_label.get(qid, "") for qid in id_list if lang_label.get(qid, "")]

        sw_names = map_names(sw_ids)
        wr_names = map_names(wr_ids)
        nv_names = map_names(nv_ids)
        wk_names = map_names(wk_ids)

        # unified, deduped
        all_names = []
        for part in (sw_names, wr_names, nv_names, wk_names):
            for n in part:
                if n and n not in all_names:
                    all_names.append(n)

        rows.append([
            author_qid,
            "|".join(sw_names),
            "|".join(wr_names),
            "|".join(nv_names),
            "|".join(wk_names),
            "|".join(all_names),
        ])

    write_csv(rows, OUTPUT_FILE)
    print(f"✅ Saved → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
