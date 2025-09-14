#####------------------------------------------------------------------------Matching Trismegistos' exclusive authors with existing (all) MEDIATE authors (based on VIAF IDs) (CSV)-----------------------------------------------------------------######

### Step 0: Importing necessary libraries
import os
import json
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import traceback
from datetime import datetime
from collections import defaultdict

### Step 1: Defining important file paths, directories and variables

## 1.1. File paths

ALL_MEDIATE_AUTHORS_RAW_TABLE_JSON = r'path_to\use-case-2\input\initial_author_lists\mediate\json\mediate_all_authors_table_raw\mediate_all_authors_table_raw.json'
EXCLUSIVE_TRISMEGISTOS_AUTHORS_CSV = r'path_to\use-case-2\output\authors_csv\04_comparing_mediate_and_trismegistos_ancient_authors_qids\04_last\04_20250914_exclusive_trismegistos_authors_qids.csv'
MEDIATE_ANCIENT_AUTHORS_WIKI_LABELLED = r'path_to\use-case-2\output\authors_csv\02_mediate_ancient_authors_csv_wiki\02_last\02_20250912_mediate_ancient_authors_wiki_labelled_last.csv'

## 1.2. Directories

OUTPUT_CSV_DIR = r'path_to\use-case-2\output\authors_csv'
MEDIATE_AUTHORS_JSON_DIR = r'path_to\use-case-2\input\initial_author_lists\mediate\json'
ERROR_LOG_DIR = r'path_to\use-case-2\output\error_logs'

## 1.3. SPARQL Setup

SPARQL_ENDPOINT = SPARQLWrapper("https://query.wikidata.org/sparql",
                       agent="your_role - your_email@email.com")

## 1.4. Other

INTERMEDIATE_TAG = '05_intermediate'
LAST_TAG = '05_last'


### Step 2: Defining the functions

## 2.1.: Processing the MEDIATE JSON table - containing all authors, authors (attributed) and authors (possible) to standardize VIAF IDs

def modifying_viaf_id_json(all_mediate_authors_raw_table_json, mediate_authors_json_dir, error_log_dir, source='mediate'):
    """
Description:
Finds the VIAF cluster ID field in the raw MEDIATE JSON table (containing all authors) and extracts the numeric value, then creates an additional field for each entry and saves these formatted entries to a new JSON file.
Rationale: we can then retrieve VIAF IDs in their numeric form from Wikidata for each exclusive Trismegistos author based on that author's QID.

Arguments:
all_mediate_authors_raw_table_json: the raw MEDIATE JSON table containing information regarding all authors, authors (attributed) and authors (possible) found on MEDIATE database.
mediate_authors_json_dir: directory where the MEDIATE JSON tables can be saved (as input for future operations)
error_log_dir: directory where the potentially encountered errors should be saved as CSV.
source='mediate': just a default tag for naming purposes

Returns:
formatted_mediate_json_table_path: the path to the new JSON table containing formatted information regarding all authors, authors (attributed) and authors (possible) found on MEDIATE database (including viaf_id)
"""


    # creating timestamp for naming purposes
    timestamp = datetime.now().strftime('%Y%m%d')

    # allowing for error logging
    error_log = []

    # loading the json file
    with open(all_mediate_authors_raw_table_json, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    print(f"[i] {source}'s table of authors (JSON) successfully loaded.")

    # computing number of entries for tracking purposes
    total_entries = len(data)
    
    # adding the viaf_id field containing the modified content from "VIAF ID (https://viaf.org)"
    for entry_nb, entry in enumerate(data, 1):
        try:
            print(f"[i] [{entry_nb}/{total_entries}] Processing entry #{entry_nb}: retrieving its VIAF ID and extracting viaf_id format.")
            
            
            viaf_url = entry.get("VIAF ID (https://viaf.org)")
            if viaf_url:
                viaf_id = viaf_url.rstrip('/').split('/')[-1]
                entry["viaf_id"] = viaf_id
                print(f"|Y| [{entry_nb}/{total_entries}] Successfully formatted the VIAF cluster ID of entry #{entry_nb} and saved it as viaf_id.")
            else:
                entry["viaf_id"] = None
        
        except Exception as e:
            print(f"|!| Could not process {entry} VIAF ID: {e}. Saving to error_log.")
            error_log.append({
                "entry_id": entry_nb,
                "error": str(e),
                "entry": entry
                })

    print(f"[i] Done processing the {total_entries} entries of the raw MEDIATE JSON table. Saving the formatted entries to a new JSON file (also useful because the raw JSON table is inoperable).")

    # saving everything back to a new JSON file
    # creating the appropriate subdirectory and naming file
    formatted_mediate_authors_json_subdir = f"{source}_all_authors_table_formatted_viaf_ids"
    formatted_mediate_authors_json_subdir_path = os.path.join(mediate_authors_json_dir, formatted_mediate_authors_json_subdir)
    os.makedirs(formatted_mediate_authors_json_subdir_path,exist_ok=True)
    formatted_mediate_json_table = f"{source}_all_authors_table_formatted_viaf_ids.json"
    formatted_mediate_json_table_path = os.path.join(formatted_mediate_authors_json_subdir_path, formatted_mediate_json_table)
    
    # saving formatted MEDIATE JSON table
    with open(formatted_mediate_json_table_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    
    print(f"|Y| New MEDIATE JSON table containing formatted viaf_ids (for all authors) successfully saved to file. See {formatted_mediate_json_table_path}.\n")

    # saving error information (if any)
    if error_log:
        print(f"|!| Encountered a total of {len(error_log)} errors. Saving the error_log.")
        error_log_subdir = f"05_{source}_authors_json_table_viaf_ids"
        error_log_subdir_path = os.path.join(error_log_dir, error_log_subdir)
        os.makedirs(error_log_subdir_path, exist_ok=True)
        error_log_name = f"05_{timestamp}_{source}_authors_json_table_viaf_ids_errors.csv"
        error_log_path = os.path.join(error_log_subdir_path, error_log_name)
        df_error_log = pd.DataFrame(error_log)
        df_error_log.to_csv(error_log_path, index=False)
        print(f"[i] error_log saved to {error_log_path}.")
    
    print(f"[i] Done sorting and processing VIAF IDs from {source}'s authors table (JSON).")

    return formatted_mediate_json_table_path

## 2.2.: retrieving the viaf_ids of the authors in the exclusive_trismegistos_authors_qids

def viaf_ids_trismegistos_authors(exclusive_trimegistos_authors_csv, output_csv_dir, sparql_endpoint, error_log_dir):
    """
Description:
This functions queries Wikidata for the possible VIAF cluster IDs associated with the set of exclusive Trismegistos authors, using their QIDs (obtained previously).
It saves the results as a separate (enriched) CSV file. The main 'difficulty' is that multiple VIAF IDs can be associated with a single QID. 
The query will then return one row per QID - VIAF ID pair. The various VIAF IDs returned for the same QIDs are saved as lists, using a defaultdict(list) to map them appropriately.

Arguments: 
exclusive_trismegistos_authors_csv (str): the path to the list containing the set of exclusive Trismegistos authors and their associated data (after comparison with the MEDIATE ancient authors' list).
output_csv_dir (str): path to the directory where the resulting enriched table of exclusive Trismegistos authors (with their respective VIAF cluster IDs) should be saved.
sparql_endpoint (SPARQLWrapper): SPARQLWrapper containing endpoint and agent.
error_log_dir (str): path to the directory where the potentially encountered errors should be saved as CSV.


Returns:
trismegistos_exclusive_authors_w_viaf_ids_csv_path (str): the path to the list of exclusive Trismegistos authors enriched with VIAF cluster IDs.

"""
    # creating a timestamp for naming purposes
    timestamp = datetime.now().strftime('%Y%m%d')

    # allowing to log errors
    error_log = []

    # loading trismegistos_authors_list as a dataframe
    df_exclusive_trismegistos_authors = pd.read_csv(exclusive_trimegistos_authors_csv)

    # getting the list of Qidentifiers from those authors to query them on Wikidata
    exclusive_qids_list = df_exclusive_trismegistos_authors["q_identifier"].tolist()

    # formatting the list so that it is usable in a SPARQL query
    values_block = " ".join(f"wd:{qid}" for qid in exclusive_qids_list)

    # setting the Wikidata query to retrieve the VIAF cluster IDs of those authors
    # writing the query
    query = f"""
    
    SELECT ?item ?itemLabelEN ?viafID
    
    WHERE {{
    VALUES ?item {{ {values_block} }}

    OPTIONAL {{
    ?item wdt:P214 ?viafID.
    }}

    OPTIONAL {{
    ?item rdfs:label ?itemLabelEN.
    FILTER(LANG(?itemLabelEN) = "en")
    }}
    
    }}
    """

    # calling the Wikidata query
    sparql_endpoint.setQuery(query)
    sparql_endpoint.setReturnFormat(JSON)
    results = sparql_endpoint.query().convert()

    # quick results check (commented out)
    # print(json.dumps(results, indent=2)) 

    # saving the data of the results as a dictionary of lists (QIDs as keys and lists of viaf_ids as values)
    # this step is necessary because if multiple VIAF cluster IDs available for same QID, the query will return one row per VIAF ID associated with this QID (hence several rows for the same QID)
    qids_to_viaf_ids_dict = defaultdict(list) # creates a special type of dictionary where each key automatically gets an empty list as value - if a key is accessed that does not exist, it creates it

    for result in results["results"]["bindings"]:
        try:
            # retrieving q_identifier from the URI
            q_identifier = result["item"]["value"].split("/")[-1]  # QID
            # retrieving the corresponding viaf_id (if available)
            viaf_id = result.get("viafID", {}).get("value")

            # adding to dictionary while preventing duplicate VIAF cluster IDs for the same QID
            if viaf_id and viaf_id not in qids_to_viaf_ids_dict[q_identifier]:
                qids_to_viaf_ids_dict[q_identifier].append(viaf_id)


        except Exception as e:
            print(f'|!| Ran into an error processing QID {result["item"]["value"].split("/")[-1]}: {e}. Saving error to error_log.')
            error_log.append({
            "q_identifier": result["item"]["value"].split("/")[-1],
            "error": str(e),
            "traceback": traceback.format_exc()
            })

    # creating a copy of the initial df_unique_trismegistos to amend it
    df_exclusive_trismegistos_authors_w_viaf_ids = df_exclusive_trismegistos_authors.copy()
    
    # adding the viaf_ids lists for each author to df_unique_trismegistos_viaf_ids (mapping thanks to dictionary)
    df_exclusive_trismegistos_authors_w_viaf_ids['viaf_id'] = df_exclusive_trismegistos_authors_w_viaf_ids['q_identifier'].map(lambda qid: qids_to_viaf_ids_dict.get(qid, [])) # if a given QID has no corresponding VIAF ID, here it will be filled with empty list.

    # we need to format these lists (JSON) before saving the df, otherwise potential problem when accessing and using later (read as strings, not lists)
    df_exclusive_trismegistos_authors_w_viaf_ids['viaf_id'] = df_exclusive_trismegistos_authors_w_viaf_ids['viaf_id'].apply(json.dumps) # don't forget to use json.loads when you use it in the next function

    # identifying missing viaf_ids rows
    df_exclusive_trismegistos_authors_no_viaf_ids = df_exclusive_trismegistos_authors_w_viaf_ids[df_exclusive_trismegistos_authors_w_viaf_ids['viaf_id'] == '[]'] # After json.dumps(), empty lists become the string '[]', so missing viaf_ids are those with '[]' (string)

    # getting an overview of the missing viaf_ids rows
    print(f"|!| Unfortunately, {len(df_exclusive_trismegistos_authors_no_viaf_ids)} authors (QIDs) had no corresponding VIAF cluster IDs on Wikidata. Saving them in a separate CSV.\n")
    print(df_exclusive_trismegistos_authors_no_viaf_ids.shape)
    print(df_exclusive_trismegistos_authors_no_viaf_ids.head())

    # saving the df_no_viaf_ids to CSV
    # creating appropriate subdir and naming
    output_csv_subdir = '05_matching_exclusive_trismegistos_authors_to_mediate_authors'
    exclusive_trismegistos_authors_qids_to_viaf_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, INTERMEDIATE_TAG)
    os.makedirs(exclusive_trismegistos_authors_qids_to_viaf_subdir_path, exist_ok=True)

    exclusive_trismegistos_authors_no_viaf_ids_csv_name = f"05_{timestamp}_exclusive_trismegistos_authors_no_viaf_ids.csv"
    exclusive_trismegistos_authors_no_viaf_ids_csv_path = os.path.join(exclusive_trismegistos_authors_qids_to_viaf_subdir_path, exclusive_trismegistos_authors_no_viaf_ids_csv_name )
    df_exclusive_trismegistos_authors_no_viaf_ids.to_csv(exclusive_trismegistos_authors_no_viaf_ids_csv_path, index=False)
    print(f"[i] Successfully saved df_no_viaf_rows to {exclusive_trismegistos_authors_no_viaf_ids_csv_path}. Dropping these rows from main df_exclusive_trismegistos_authors_qids_to_viaf_ids.")

    # dropping the no_viaf_rows from the df_exclusive_trismegistos_authors_w_viaf_ids
    df_exclusive_trismegistos_authors_w_viaf_ids = df_exclusive_trismegistos_authors_w_viaf_ids[df_exclusive_trismegistos_authors_w_viaf_ids['viaf_id'] != '[]']
    print(f"[i] Found corresponding viaf_ids for {len(df_exclusive_trismegistos_authors_w_viaf_ids)} authors (QIDs) from the set of exclusive Trismegistos authors through the Wikidata query. Saving them to a new CSV.\n")

    # getting an overview of the df_unique_trismegistos_viaf_ids
    print(df_exclusive_trismegistos_authors_w_viaf_ids.shape)
    print(df_exclusive_trismegistos_authors_w_viaf_ids.head())

    # saving the df_unique_trismegistos_viaf_ids dataframe to CSV
    trismegistos_exclusive_authors_w_viaf_ids_csv_name = f"05_{timestamp}_exclusive_trismegistos_authors_with_viaf_ids.csv"
    trismegistos_exclusive_authors_w_viaf_ids_csv_path = os.path.join(exclusive_trismegistos_authors_qids_to_viaf_subdir_path, trismegistos_exclusive_authors_w_viaf_ids_csv_name)
    df_exclusive_trismegistos_authors_w_viaf_ids.to_csv(trismegistos_exclusive_authors_w_viaf_ids_csv_path, index=False)
    print(f"[i] Successfully saved df_trismegistos_unique_qids_to_viaf to {trismegistos_exclusive_authors_w_viaf_ids_csv_path}.")

    # saving error_log if not empty
    if error_log:
        print(f"|!| Encountered a total of {len(error_log)} errors trying to retrieve the VIAF cluster IDs corresponding to the exclusive Trismegistos authors'QIDs. Saving the error_log.")
        error_log_subdir = f"05_matching_exclusive_trismegistos_authors_to_mediate_authors"
        error_log_subdir_path = os.path.join(error_log_dir, error_log_subdir)
        os.makedirs(error_log_subdir_path, exist_ok=True)
        error_log_name = f"05_{timestamp}_exclusive_trismegistos_authors_qids_to_viaf_ids.csv"
        error_log_path = os.path.join(error_log_subdir_path, error_log_name)
        df_error_log = pd.DataFrame(error_log)
        df_error_log.to_csv(error_log_path, index=False)
        print(f"[i] error_log saved to {error_log_path}.")

    # last message
    print(f"[i] Done processing trismegistos_unique_qids.csv and retrieving corresponding (existing) VIAF IDs from Wikidata.")

    return trismegistos_exclusive_authors_w_viaf_ids_csv_path

## 2.3.: Matching the viaf_ids retrieved for trismegistos_exclusive_authors_w_viaf_ids to the VIAF cluster IDs found in the formatted MEDIATE authors JSON table, and saving matches to a separate CSV (and concatenating with main MEDIATE CSV).

def matching_viaf_ids_trismegistos_exclusive_to_mediate_authors_JSON_table(trismegistos_exclusive_authors_w_viaf_ids_csv, mediate_ancient_authors_wiki_labelled_csv, formatted_mediate_json_table, output_csv_dir, error_log_dir):
    """
Description:
This function uses the VIAF cluster IDs collected in 2.2. for exclusive Trismegistos authors to match them against all MEDIATE authors, based on the formatted JSON table holding the information on all entries from MEDIATE (including viaf_id).
It then concatenates the results with the mediate_ancient_authors_labelled_wiki CSV to obtain the 'last', extended table of MEDIATE ancient authors
(since we add previously undetected and nonetheless existingMEDIATE authors thanks to the comparison with the Trismegistos list) at which we could arrive 'automatically' or 'semi-automatically'.

Arguments:
trismegistos_exclusive_authors_w_viaf_ids_csv (str): path to the list containing the set of exclusive Trismegistos authors for which VIAF IDs have been found in 2.2.
mediate_ancient_authors_wiki_labelled_csv (str): path to the list of MEDIATE authors enriched with data from Wikidata (QIDs, labels, alises and writing languages)
formatted_mediate_json_table (str): the path to the new JSON table (obtained in 2.1.) containing formatted information regarding all authors, authors (attributed) and authors (possible) found on MEDIATE database (including viaf_id)
output_csv_dir (str): path to the directory where the resulting last concatenated MEDIATE DataFrame (and all other intermediate results) should be saved.
error_log_dir (str): path to the directory where the potentially encountered errors should be saved as CSV.

Returns:
concatenated_mediate_ancient_authors_csv_path: path to the last CSV holding the concatenated DataFrames with initial enriched MEDIATE authors list (with QIDs, labels, aliases, writing languages) and with matched exclusive Trismegistos authors based on their retrieved VIAF cluster IDs.

"""


    # creating a timestamp for naming purposes
    timestamp = datetime.now().strftime('%Y%m%d')

    # allowing to log errors
    error_log = []

    # loading mediate_ancient_authors_wiki_labelled_csv into a df (which will serve as basis for concatenation later)
    # first defining the datatypes of some columns
    mediate_datatypes = {
        "english_label": "string",
        "french_label": "string",
        "latin_label": "string",
        "q_identifier": "string",
        "mediate_label": "string",
        "viaf_id": "string",
        "mediate_nb_items": "Int64",
        "mediate_nb_collections": "Int64",
    }

    # loading the CSV as df with correct datatypes
    df_mediate_ancient_authors_wiki_labelled = pd.read_csv(mediate_ancient_authors_wiki_labelled_csv, dtype=mediate_datatypes)

    # safely loading json_formatted columns
    df_mediate_ancient_authors_wiki_labelled ['english_aliases'] = df_mediate_ancient_authors_wiki_labelled['english_aliases'].apply(json.loads)
    df_mediate_ancient_authors_wiki_labelled ['french_aliases'] = df_mediate_ancient_authors_wiki_labelled['french_aliases'].apply(json.loads)
    df_mediate_ancient_authors_wiki_labelled ['latin_aliases'] = df_mediate_ancient_authors_wiki_labelled['latin_aliases'].apply(json.loads)
    df_mediate_ancient_authors_wiki_labelled ['writing_languages'] = df_mediate_ancient_authors_wiki_labelled['writing_languages'].apply(json.loads)

    # loading trismegistos_exclusive_authors_w_viaf_ids_csv into a df
    # first defining the datatypes of some columns
    trismegistos_datatypes = {
        "english_label": "string",
        "french_label": "string",
        "latin_label": "string",
        "q_identifier": "string",
        "trismegistos_label": "string",
        "trismegistos_id": "string"
    }

    # loading the CSV as df with correct datatypes
    df_trismegistos_exclusive_authors_w_viaf_ids = pd.read_csv(trismegistos_exclusive_authors_w_viaf_ids_csv, dtype=trismegistos_datatypes)

    # safely loading json_formatted columns
    df_trismegistos_exclusive_authors_w_viaf_ids['english_aliases'] = df_trismegistos_exclusive_authors_w_viaf_ids['english_aliases'].apply(json.loads)
    df_trismegistos_exclusive_authors_w_viaf_ids['french_aliases'] = df_trismegistos_exclusive_authors_w_viaf_ids['french_aliases'].apply(json.loads)
    df_trismegistos_exclusive_authors_w_viaf_ids['latin_aliases'] = df_trismegistos_exclusive_authors_w_viaf_ids['latin_aliases'].apply(json.loads)
    df_trismegistos_exclusive_authors_w_viaf_ids['writing_languages'] = df_trismegistos_exclusive_authors_w_viaf_ids['writing_languages'].apply(json.loads)

    # and do not forget to apply json.loads to the lists of VIAF IDs collected in 2.2.
    df_trismegistos_exclusive_authors_w_viaf_ids['viaf_id'] = df_trismegistos_exclusive_authors_w_viaf_ids['viaf_id'].apply(json.loads)

    # loading the data from the formatted_mediate_json_table from 2.1.
    with open(formatted_mediate_json_table, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # creating the dictionary to hold Trismegistos q_identifiers as keys and viaf_id lists as values (to then transform target_viaf_id into a matching q_id)
    trismegistos_viaf_ids_to_qids_dict = df_trismegistos_exclusive_authors_w_viaf_ids.set_index('q_identifier')['viaf_id'].to_dict()

    # computing total_entries from formatted_mediate_json_table for tracking purposes
    total_entries = len(data)

    # defining a function to match QIDs of exclusive Trismegistos authors if one corresponding VIAF ID (in dict values) matches the VIAF cluster ID of one of the formatted_mediate_json_table viaf_id
    
    # defining the function
    def find_qids_by_viaf_id(qids_to_viaf_dict, target_viaf_id, multiple_qids_list):
        matched_qids = [q_identifier for q_identifier, viaf_ids in qids_to_viaf_dict.items() if target_viaf_id in viaf_ids]
        if len(matched_qids) > 1:
            matched_tm_labels = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'].isin(matched_qids), 'trismegistos_label'].tolist()
            multiple_qids_list.append({
                "viaf_id": target_viaf_id,
                "matched_trismegistos_qids": matched_qids,
                "matched_trismegistos_labels": matched_tm_labels,
            })
            print(f'|!| Warning: found more than one matching QID for target viaf_id: {target_viaf_id} - {matched_qids}. Saving as dictionary in multiple_qids_matched = [] to then save as separate CSV for later review.')
            return None
        elif len(matched_qids) == 0:
            print(f'|!| No matching qid found for {target_viaf_id}.')
            return None
        elif len(matched_qids) == 1:
            print(f'[i] Found exactly one matching QID for target viaf_id: {target_viaf_id} - {matched_qids}.')
            return matched_qids[0]

    # preparing a list to hold matching results and then use them as df
    viaf_ids_to_qids_unique_matches = []

    # initialising a list to deal with cases where a single VIAF cluster ID would match multiple QIDs
    multiple_qids_matched = []

    # creating a list of all viaf_ids present in the formatted_mediate_authors_table_json to iterate through them
    for entry_nb, entry in enumerate(data, 1):
        try:
            print(f"[i] [{entry_nb}/{total_entries}] Processing entry #{entry_nb}: retrieving its VIAF ID from formatted MEDIATE JSON table and matching against those in trismegistos_viaf_ids_to_qids_dict.")
                
            viaf_id = entry.get("viaf_id") # hopefully all entries have a viaf_id. If they don't it means they either had no VIAF ID to begin with or something went wrong changing the format. In any case not interested in logging (not necessarily ancient authors).
            short_name = entry.get("short_name")

            if not viaf_id:
                print(f"|!| [{entry_nb}/{total_entries}] Entry {entry_nb}, for author ({short_name}) has no VIAF ID. Skipping.")
                continue

            q_identifier = find_qids_by_viaf_id(trismegistos_viaf_ids_to_qids_dict, viaf_id, multiple_qids_matched)

            if not q_identifier:
                print(f"|!| [{entry_nb}/{total_entries}] No matching q_identifier found for VIAF ID {viaf_id} (entry {entry_nb}, author ({short_name})).") # still not interested in logging because taking from 'all MEDIATE authors' file
                continue
            
            print(f"|Y| [{entry_nb}/{total_entries}] Matching QID found for or VIAF ID {viaf_id} (entry {entry_nb}, author ({short_name}))!")
            
            # this part will only run for VIAF cluster IDs from formatted MEDIATE JSON table that have been matched with exclusive Trismegistos authors

            english_label = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'] == q_identifier, 'english_label'] # returns a pandas Series
            english_label = english_label.iloc[0] if not english_label.empty else None
            
            french_label = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'] == q_identifier, 'french_label']
            french_label = french_label.iloc[0] if not french_label.empty else None
            
            latin_label = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'] == q_identifier, 'latin_label']
            latin_label = latin_label.iloc[0] if not latin_label.empty else None

            english_aliases = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'] == q_identifier, 'english_aliases']
            english_aliases = english_aliases.iloc[0] if not english_aliases.empty else []

            french_aliases = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'] == q_identifier, 'french_aliases']
            french_aliases = french_aliases.iloc[0] if not french_aliases.empty else []
            
            latin_aliases = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'] == q_identifier, 'latin_aliases']
            latin_aliases = latin_aliases.iloc[0] if not latin_aliases.empty else []

            writing_languages = df_trismegistos_exclusive_authors_w_viaf_ids.loc[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'] == q_identifier, 'writing_languages']
            writing_languages = writing_languages.iloc[0] if not writing_languages.empty else []

            mediate_nb_items = entry.get("# items")

            mediate_nb_collections = entry.get("# collections")

            viaf_ids_to_qids_unique_matches.append({
                'english_label': english_label,
                'french_label': french_label,
                'latin_label': latin_label,
                'q_identifier': q_identifier,
                'mediate_label': short_name,
                'viaf_id': viaf_id,
                'mediate_nb_items': mediate_nb_items,
                'mediate_nb_collections': mediate_nb_collections,
                'english_aliases': english_aliases if english_aliases is not None else [],
                'french_aliases': french_aliases if french_aliases is not None else [],
                'latin_aliases': latin_aliases if latin_aliases is not None else [],
                'writing_languages': writing_languages if writing_languages is not None else []
                })
            
        except Exception as e:
            error_type = type(e).__name__
            error_message = str(e)
            tb = traceback.format_exc()
            print(f"|!| Could not process entry #{entry_nb}: {e}. Saving to error_log.")
            error_log.append({
                "entry_id": entry_nb,
                "error_type": error_type,
                "error_message": error_message,
                "traceback": tb,
                "entry": entry
                })
    
    print(f"[i] Done processing {total_entries} entries from the formatted_mediate_table_json. Will now save error_log. Will then save the viaf_ids_to_qids_matches as separate CSV and then concatenate with mediate_ancient_authors_wiki_labelled_csv into final CSV.\n")

    # saving errors if any
    if error_log:
        print(f"[i] Ran into a total of {len(error_log)} errors while processing {total_entries} entries from the formatted_mediate_authors_table_json and trying to match some viaf_ids against the trismegistos_exclusive_authors_w_viaf_ids_csv. Will now save to CSV.")
        df_error_log = pd.DataFrame(error_log)
        error_log_subdir = f"05_matching_exclusive_trismegistos_authors_to_mediate_authors"
        error_log_subdir_path = os.path.join(error_log_dir, error_log_subdir)
        os.makedirs(error_log_subdir_path, exist_ok=True)
        error_log_csv_name = f"05_{timestamp}_matching_tm_exclusive_authors_to_all_mediate_authors_errors.csv"
        error_log_csv_path = os.path.join(error_log_subdir_path, error_log_csv_name)
        df_error_log.to_csv(error_log_csv_path, index=False)
        print(f"[i] Saved error_log to {error_log_csv_path}.")

    # saving viaf_ids_to_qids_matches to CSV
    # first safely transforming the list holding the data into a df
    if len(viaf_ids_to_qids_unique_matches) == 0:
        print("|!| Warning: No matching authors found. Creating empty DataFrame with expected columns.")
        columns = ['english_label', 'french_label', 'latin_label', 'q_identifier', 'mediate_label', 'viaf_id',
               'mediate_nb_items', 'mediate_nb_collections', 'english_aliases', 'french_aliases', 'latin_aliases', 'writing_languages']
        df_matched_exclusive_tm_authors_to_mediate_authors = pd.DataFrame(columns=columns)
    else:
        df_matched_exclusive_tm_authors_to_mediate_authors = pd.DataFrame(viaf_ids_to_qids_unique_matches)

    # making sure some fields are saved properly
    df_matched_exclusive_tm_authors_to_mediate_authors['english_aliases'] = df_matched_exclusive_tm_authors_to_mediate_authors['english_aliases'].apply(json.dumps)
    df_matched_exclusive_tm_authors_to_mediate_authors['french_aliases'] = df_matched_exclusive_tm_authors_to_mediate_authors['french_aliases'].apply(json.dumps)
    df_matched_exclusive_tm_authors_to_mediate_authors['latin_aliases'] = df_matched_exclusive_tm_authors_to_mediate_authors['latin_aliases'].apply(json.dumps)
    df_matched_exclusive_tm_authors_to_mediate_authors['writing_languages'] = df_matched_exclusive_tm_authors_to_mediate_authors['writing_languages'].apply(json.dumps)

    # creating the appropriate subdirectories
    output_csv_subdir_name = "05_matching_exclusive_trismegistos_authors_to_mediate_authors"
    output_csv_subdir_path = os.path.join(output_csv_dir, output_csv_subdir_name)
    os.makedirs(output_csv_subdir_path, exist_ok=True)
    matching_tm_exclusive_authors_to_mediate_authors_intermediate_subdir_path = os.path.join(output_csv_dir, output_csv_subdir_path, INTERMEDIATE_TAG)
    os.makedirs(matching_tm_exclusive_authors_to_mediate_authors_intermediate_subdir_path, exist_ok=True)

    # saving the file containing df_matched_tm_exclusive_authors_to_mediate_authors
    matched_exclusive_tm_authors_to_mediate_authors_name = f"05_{timestamp}_matched_exclusive_trismegistos_authors_to_mediate_authors.csv"
    matched_exclusive_tm_authors_to_mediate_authors_path = os.path.join(matching_tm_exclusive_authors_to_mediate_authors_intermediate_subdir_path, matched_exclusive_tm_authors_to_mediate_authors_name)
    df_matched_exclusive_tm_authors_to_mediate_authors.to_csv(matched_exclusive_tm_authors_to_mediate_authors_path, index=False)
    print(f"|Y| Successfully saved df_matched_exclusive_tm_authors_to_mediate_authors to CSV: {matched_exclusive_tm_authors_to_mediate_authors_path}.")

    if len(multiple_qids_matched) > 0:
        print(f"[i] Found {len(multiple_qids_matched)} cases where one VIAF cluster ID from formatted MEDIATE JSON table matched with two exclusive Trismegistos authors. Will now create df and save as separate CSV.")
        # creating and saving the file containing the problematic multipled_qids_matched list
        # creating the df
        df_multiple_exclusive_tm_authors_matched = pd.DataFrame(multiple_qids_matched)

        # naming the file
        multiple_exclusive_tm_authors_matched_name = f"05_{timestamp}_multiple_exclusive_tm_authors_qids_to_unique_viaf_id.csv"
        multiple_exclusive_tm_authors_matched_path = os.path.join(matching_tm_exclusive_authors_to_mediate_authors_intermediate_subdir_path, multiple_exclusive_tm_authors_matched_name)
        df_multiple_exclusive_tm_authors_matched.to_csv(multiple_exclusive_tm_authors_matched_path, index=False)
        print(f"|Y| Successfully saved df_multiple_exclusive_tm_authors_matched to: {multiple_exclusive_tm_authors_matched_path}")

        # counting the number of unique qids that are in this df_multiple_exclusive_tm_authors_matched to exclude them in next step (finding remaining 'unmatched' qids)
        matched_tm_multiple_qids = set(qid for qids in df_multiple_exclusive_tm_authors_matched['matched_trismegistos_qids'] for qid in qids)
    else:
        print(f"|Y| No cases of 'multiple TM authors' matched encountered while processing the VIAF cluster IDs found in the formatted MEDIATE JSON table.")
        matched_tm_multiple_qids = set()


    # saving a file containing tm_exclusive_authors that were not matched with MEDIATE authors
    # first identifying unmatched q_identifiers
    all_tm_qids = set(df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'])
    matched_tm_unique_qids = set(df_matched_exclusive_tm_authors_to_mediate_authors['q_identifier'])
    
    unmatched_tm_exclusive_qids = all_tm_qids - matched_tm_unique_qids - matched_tm_multiple_qids

    # creating a df to hold unmatched_exclusive_trismegistos_authors and saving it as a separate CSV (to review it manually later)
    df_unmatched_exclusive_tm_qids = df_trismegistos_exclusive_authors_w_viaf_ids[df_trismegistos_exclusive_authors_w_viaf_ids['q_identifier'].isin(unmatched_tm_exclusive_qids)]

    # saving to the subdir created previously for matched tm_unique_authors
    unmatched_tm_authors_csv_name = f"05_{timestamp}_unmatched_exclusive_trismegistos_authors.csv"
    unmatched_tm_authors_csv_path = os.path.join(matching_tm_exclusive_authors_to_mediate_authors_intermediate_subdir_path, unmatched_tm_authors_csv_name)
    df_unmatched_exclusive_tm_qids.to_csv(unmatched_tm_authors_csv_path, index=False)

    # will now proceed to create the last_mediate_ancient_authors_csv by merging df_mediate_ancient_authors_wiki_labelled with df_matched_exclusive_tm_authors_to_mediate_authors
    # making sure some fields are saved properly before proceeding to merge both df
    df_mediate_ancient_authors_wiki_labelled ['english_aliases'] = df_mediate_ancient_authors_wiki_labelled['english_aliases'].apply(json.dumps)
    df_mediate_ancient_authors_wiki_labelled ['french_aliases'] = df_mediate_ancient_authors_wiki_labelled['french_aliases'].apply(json.dumps)
    df_mediate_ancient_authors_wiki_labelled ['latin_aliases'] = df_mediate_ancient_authors_wiki_labelled['latin_aliases'].apply(json.dumps)
    df_mediate_ancient_authors_wiki_labelled ['writing_languages'] = df_mediate_ancient_authors_wiki_labelled['writing_languages'].apply(json.dumps)

    # merging df_matched_exclusive_tm_authors_to_mediate_authors with df_mediate_ancient_authors_wiki_labelled
    df_concatenated_mediate_ancient_authors = pd.concat([df_mediate_ancient_authors_wiki_labelled, df_matched_exclusive_tm_authors_to_mediate_authors], ignore_index=True)

    # making sure the mediate_nb_items and mediate_nb_collections columns are read as integers
    df_concatenated_mediate_ancient_authors['mediate_nb_collections'] = pd.to_numeric(df_concatenated_mediate_ancient_authors['mediate_nb_collections'], errors='coerce').fillna(0).astype(int)
    df_concatenated_mediate_ancient_authors['mediate_nb_items'] = pd.to_numeric(df_concatenated_mediate_ancient_authors['mediate_nb_items'], errors='coerce').fillna(0).astype(int)  # turns NaN from invalid strings into 0s
    
    # sorting depending on nb_collections
    df_concatenated_mediate_ancient_authors = df_concatenated_mediate_ancient_authors.sort_values(by='mediate_nb_collections', ascending=False)

    # getting an overview of the meged df
    print(df_concatenated_mediate_ancient_authors.shape)
    print(df_concatenated_mediate_ancient_authors.head())

    # creating the appropriate subdirectory
    concatenated_mediate_ancient_authors_last_subdir_path = os.path.join(output_csv_subdir_path, LAST_TAG)
    os.makedirs(concatenated_mediate_ancient_authors_last_subdir_path, exist_ok=True)
    concatenated_mediate_ancient_authors_last_csv_name = f"05_{timestamp}_concatenated_mediate_ancient_authors.csv"
    concatenated_mediate_ancient_authors_csv_path = os.path.join(concatenated_mediate_ancient_authors_last_subdir_path, concatenated_mediate_ancient_authors_last_csv_name)

    # saving the df_concatenated_mediate_ancient_authors to CSV
    df_concatenated_mediate_ancient_authors.to_csv(concatenated_mediate_ancient_authors_csv_path, index=False)

    print(f"|Y| Done matching viaf_ids from Trismegistos' exclusive authors to (all) existing MEDIATE authors based on formatted JSON table. Check result: {concatenated_mediate_ancient_authors_csv_path}.")

    return concatenated_mediate_ancient_authors_csv_path

### Step 3: Calling all three functions

if __name__ == "__main__":
    print(f">>>> Starting the script to match unique trismegistos authors with existing MEDIATE authors <<<<")
    try:
        print(f">>>> [1/3] Calling first function: modifying_viaf_id_json() to process the raw MEDIATE JSON table and extract formatted VIAF cluster IDs.<<<<")
        new_mediate_json_path = modifying_viaf_id_json(
            all_mediate_authors_raw_table_json=ALL_MEDIATE_AUTHORS_RAW_TABLE_JSON,
            mediate_authors_json_dir=MEDIATE_AUTHORS_JSON_DIR,
            error_log_dir=ERROR_LOG_DIR
        )

        print(f">>>> [2/3] Calling second function: viaf_ids_trismegistos_authors() to retrieve VIAF IDs corresponding to exclusive TM authors from Wikidata.<<<<")
        trismegistos_qids_to_viaf_csv_path = viaf_ids_trismegistos_authors(
            exclusive_trimegistos_authors_csv=EXCLUSIVE_TRISMEGISTOS_AUTHORS_CSV,
            output_csv_dir=OUTPUT_CSV_DIR,
            sparql_endpoint=SPARQL_ENDPOINT,
            error_log_dir=ERROR_LOG_DIR
        )
        
        print(f">>>> [3/3] Calling third function: matching_viaf_ids_trismegistos_exclusive_to_mediate_authors_JSON_table() to match exclusive TM authors to existing MEDIATE authors based on VIAF IDs.<<<<")
        final_csv_path = matching_viaf_ids_trismegistos_exclusive_to_mediate_authors_JSON_table(
            trismegistos_exclusive_authors_w_viaf_ids_csv=trismegistos_qids_to_viaf_csv_path,
            mediate_ancient_authors_wiki_labelled_csv=MEDIATE_ANCIENT_AUTHORS_WIKI_LABELLED,
            formatted_mediate_json_table=new_mediate_json_path,
            output_csv_dir=OUTPUT_CSV_DIR,
            error_log_dir=ERROR_LOG_DIR
        )

        print(f"|Y| Pipeline completed successfully. Final output saved to:\n{final_csv_path}")

    except Exception as main_e:
        print(f"|!| Pipeline failed because of the following error: {main_e}")
        traceback.print_exc()