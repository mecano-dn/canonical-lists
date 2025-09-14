#####------------------------------------------------------------------------Retrieving relevant Wikidata information for authors from the Trismegistos' Canonical list (CSV)-----------------------------------------------------------------######

### Step 0: Importing necessary libraries
import os
import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, JSON

from datetime import datetime
import traceback


### Step 1: Defining relevant directories, file paths and variables

## 1.1.: File path to initial authors' list

INPUT_AUTHORS_LIST = r'path_to\use-case-2\input\initial_author_lists\trismegistos\trismegistos_authors.csv'

## 1.2.: Source of the list
SOURCE = 'trismegistos'

## 1.3.: Relevant directories
OUTPUT_CSV_DIR = r'path_to\use-case-2\output\authors_csv'
ERROR_LOG_DIR = r'path_to\use-case-2\output\error_logs'

## 1.4. SPARQL endpoint
SPARQL = SPARQLWrapper("https://query.wikidata.org/sparql",
                       agent="your_role - your_email@email.com")
## 1.5. Other

INTERMEDIATE_TAG = '03_intermediate'
LAST_TAG = '03_last'

### Step 2: Defining the function to get the QID, the English, French and Latin labels, the English, French, Latin aliases and the writing language(s) from the authors listed

def retrieve_qids_aliases_lang_trismegistos_wikidata(input_authors_list, source, sparql_setup, output_directory, error_log_dir, specific_ids=None, nb_ids=None):

    """
Queries Wikidata for authors in a Trismegistos CSV using their TM ID, retrieving English, French and Lain labels & aliases,
and writing languages. Results are saved to a CSV. Handles partial or full lists.

Arguments:
    input_authors_list (str): Path to the CSV with 'ID' and 'Author Name' columns.
    source (str): Source tag (e.g., 'TM') used in column naming.
    sparql_setup: SPARQLWrapper setup with endpoint.
    output_directory (str): Where to save output CSV.
    error_log_dir (str): Directory to save error logs.
    specific_ids (list, optional): List of TM IDs to filter. Defaults to None.
    nb_ids (int, optional): Limit number of authors to query. Defaults to None.

Returns:
    tuple: (DataFrame of results, path to output CSV)
"""
    # initialising return objects to avoid crash
    df_ancient_authors_output = pd.DataFrame()  # empty dataframe as fallback
    output_csv_path = None
    output_name = None

    # creating the timestamp early for naming purposes
    timestamp = datetime.now().strftime('%Y%m%d')

    # initialising a list to hold possible errors
    error_log = []

    try:
        # loading the initial CSV containing the list of authors and making a copy
        df_authors_source = pd.read_csv(input_authors_list) # cannot avoid it to 'label' results later
        df_authors_input = df_authors_source.copy() # to avoid modifying anything in the original df (and CSV)

        ## (a) Preparing to query the Wikidata database
        # Deciding which authors to query: by default, querying names and aliases for all IDs in the input_csv, but allowing for specifci_ids or nb_ids

        if specific_ids is not None:
            author_ids = [str(id) for id in specific_ids if pd.notna(id)]
        else:
            if nb_ids is not None:
                author_ids = df_authors_input.iloc[:nb_ids,:]["ID"].dropna().astype(str).tolist()
            else:
                author_ids = df_authors_input["ID"].dropna().astype(str).tolist() # .dropna() removes any NaN (missing) values from the Series (from ID column)

        # exiting early if no author_ids are found to query
        if not author_ids:
            print("|!| No author_ids found. Check CSV or default parameter specific_ids = [].")
            return None

        # feeding the target author ids to the SPARQL query
        values_block = " ".join(f'"{id}"' for id in author_ids)

        # setting up the SPARQL query

        query = f"""

    SELECT ?trismegistosID ?item ?itemLabelEN ?itemLabelFR ?itemLabelLA
        (GROUP_CONCAT(DISTINCT ?aliasEnglish; SEPARATOR=", ") AS ?aliasesEnglish)
        (GROUP_CONCAT(DISTINCT ?aliasFrench; SEPARATOR=", ") AS ?aliasesFrench)
        (GROUP_CONCAT(DISTINCT ?aliasLatin; SEPARATOR=", ") AS ?aliasesLatin)
        (GROUP_CONCAT(DISTINCT ?writingLangLabelEN; SEPARATOR=", ") AS ?writingLanguages)

    WHERE {{
    
    VALUES ?trismegistosID {{ {values_block} }}
    ?item wdt:P11252 ?trismegistosID.

    OPTIONAL {{
    ?item rdfs:label ?itemLabelEN.
    FILTER(LANG(?itemLabelEN) = "en")
    }}

    OPTIONAL {{
        ?item rdfs:label ?itemLabelFR.
        FILTER(LANG(?itemLabelFR) = "fr")
    }}

    OPTIONAL {{
        ?item rdfs:label ?itemLabelLA.
        FILTER(LANG(?itemLabelLA) = "la")
    }}
      
    OPTIONAL {{
        ?item skos:altLabel ?aliasEnglish.
        FILTER(LANG(?aliasEnglish) = "en")
    }}

    OPTIONAL {{
        ?item skos:altLabel ?aliasFrench.
        FILTER(LANG(?aliasFrench) = "fr")
    }}

    OPTIONAL {{
        ?item skos:altLabel ?aliasLatin.
        FILTER(LANG(?aliasLatin) = "la")
    }}

    OPTIONAL {{
        ?item wdt:P6886 ?writingLang.
        ?writingLang rdfs:label ?writingLangLabelEN.
        FILTER(LANG(?writingLangLabelEN) = "en")
    }}

    }}
    GROUP BY ?trismegistosID ?item ?itemLabelEN ?itemLabelFR ?itemLabelLA
    """


        # Calling the query
        sparql_setup.setQuery(query)
        sparql_setup.setReturnFormat(JSON)
        results = sparql_setup.query().convert()

        # quick check (commented out)
        # print(json.dumps(results, indent=2)) 

        # checking which viaf_ids were matched or unmatched
        # collecting the matched tm_ids
        matched_tm_ids = {result["trismegistosID"]["value"] for result in results["results"]["bindings"]}
        
        # creating a list of unmatched viaf_ids
        unmatched_tm_ids = [id_ for id_ in author_ids if id_ not in matched_tm_ids]

        ## (b) Processing the results of the SPARQL query on Wikidata and saving the results as a CSV

        # saving the data of the results as a list of dictionaries before creating the dataframe
        data = []

        for result in results["results"]["bindings"]:
            try:
                tm_id = result["trismegistosID"]["value"]
                
                # getting the English, French and Latin labels for matched authors
                en_label = result.get("itemLabelEN", {}).get("value", "")
                fr_label = result.get("itemLabelFR", {}).get("value", "")
                la_label = result.get("itemLabelLA", {}).get("value", "")

                # Getting the QID for each queried author (to allow for cross-database comparison later)
                qid_uri = result.get("item", {}).get("value", "")
                qid = qid_uri.split("/")[-1] if qid_uri else None
                if not qid:
                    print(f"|!| No QID found for tm_id: {tm_id}") # unlikely since here we are only dealing with 'matched' TM IDs

                # Getting the writing language(s) for every author
                writing_lang_labels = result.get("writingLanguages", {}).get("value", "").split(", ")
                writing_languages = list(set(filter(None, writing_lang_labels)))  # remove empty strings and deduplicate

                # Getting alternative author labels (aliases) and putting them in the right format for CSV storage
                aliases_en = result.get("aliasesEnglish", {}).get("value", "").split(", ")
                aliases_fr = result.get("aliasesFrench", {}).get("value", "").split(", ")
                aliases_la = result.get("aliasesLatin", {}).get("value", "").split(", ")

                # making sure there are no duplicates for each series of aliases
                aliases_en = list(set(filter(None, aliases_en)))
                aliases_fr = list(set(filter(None, aliases_fr)))
                aliases_la = list(set(filter(None, aliases_la)))

                # mapping matched tm_ids back to the initial list to extract some information from there and use it in the final CSV
                source_row = df_authors_input.loc[df_authors_input["ID"].astype(str) == str(tm_id)]
                source_label = source_row["Author Name"].iloc[0] if not source_row.empty else None

                # Storing everything in the output data_dictionary for each author found
                data.append({
                    "english_label": en_label,
                    "french_label": fr_label,
                    "latin_label": la_label,
                    "q_identifier": qid,
                    f"{source}_label": source_label,
                    f"{source}_id": tm_id,
                    "english_aliases": aliases_en,
                    "french_aliases": aliases_fr,
                    "latin_aliases": aliases_la,
                    "writing_languages": writing_languages
                })

            # tracking errors inside the results loop
            except Exception as e:
                error_log.append({
                "tm_id": result.get("trismegistosID", {}).get("value", "N/A"),
                "error": str(e),
                "traceback": traceback.format_exc()
                })

        ## (c) Saving the results

        # creating a dataframe holding the results of the query for all queried and matched tm_ids
        df_ancient_authors_output = pd.DataFrame(data)

        # making sure that the aliases and the writing languages are correctly stored (JSON format when retrieving the query)
        df_ancient_authors_output["english_aliases"] = df_ancient_authors_output["english_aliases"].apply(json.dumps)
        df_ancient_authors_output["french_aliases"] = df_ancient_authors_output["french_aliases"].apply(json.dumps)
        df_ancient_authors_output["latin_aliases"] = df_ancient_authors_output["latin_aliases"].apply(json.dumps)

        df_ancient_authors_output["writing_languages"] = df_ancient_authors_output["writing_languages"].apply(json.dumps)

        # then don't forget to apply 
        # df = pd.read_csv("your_file.csv")
        # df["french_aliases"] = df["french_aliases"].apply(json.loads) - when you reload the CSV, otherwise will not be read as a list but as a string

        # printing overview of the results
        print(df_ancient_authors_output.head())

        # defining the subdirectory to hold the CSV
        output_csv_subdir = f'03_{source}_ancient_authors_csv_wiki'
        output_csv_subdir_path = os.path.join(output_directory, output_csv_subdir)
        os.makedirs(output_csv_subdir_path, exist_ok=True)

        trismegistos_ancient_authors_wiki_labelled_csv_subdir_path = os.path.join(output_csv_subdir_path, LAST_TAG)
        os.makedirs(trismegistos_ancient_authors_wiki_labelled_csv_subdir_path, exist_ok=True)

        # adjusting the name of the output CSV depending on source and target authors  
        if specific_ids is None and nb_ids is None:
            output_name = f'03_{timestamp}_{source}_ancient_authors_wiki_labelled.csv'
        elif specific_ids:
            match_row = df_authors_input.loc[df_authors_input["ID"].astype(str) == str(specific_ids[0])]
            author_label = match_row["Author Name"].iloc[0] if not match_row.empty else "unknown"
            output_name = f'03_{timestamp}_{source}_ancient_authors_wiki_labelled_{author_label}_{len(author_ids)}.csv'
        elif nb_ids:
            output_name = f'03_{timestamp}_{source}_ancient_authors_wiki_labelled_{nb_ids}_nb_ids.csv'
        
        # defining the output path and saving the df with the collected labels and QIDs as CSV
        trismegistos_ancient_authors_wiki_labelled_csv_path = os.path.join(trismegistos_ancient_authors_wiki_labelled_csv_subdir_path, output_name)
        df_ancient_authors_output.to_csv(trismegistos_ancient_authors_wiki_labelled_csv_path, index=False)

        print(f"|Y| {output_name} saved to {trismegistos_ancient_authors_wiki_labelled_csv_path}")

    except Exception as e:
        error_log.append({
            "tm_id": "N/A",
            "error": str(e),
            "traceback": traceback.format_exc()
        })
    
    # now dealing with the unmatched tm_ids
    if unmatched_tm_ids:
        print(f"|!| Unable to match {len(unmatched_tm_ids)} author(s) from the list with their Trismegistos IDs. Check the CSV and update manually later.")

        # saving the rows to a CSV
        # creating a df holding those rows
        df_unmatched = df_authors_input[df_authors_input["ID"].astype(str).isin(unmatched_tm_ids)].copy()

        # overview of the unmatched values
        print(df_unmatched.head())

        # creating the appropriate subdirectory for unmatched Trismegistos authors
        unmatched_authors_csv_subdir_path = os.path.join(output_csv_subdir_path, INTERMEDIATE_TAG)
        os.makedirs(unmatched_authors_csv_subdir_path, exist_ok=True)

        # naming the CSV properly
        unmatched_csv_name = f"03_{timestamp}_{source}_unmatched_ancient_authors_wiki_tm_id.csv"
        unmatched_csv_path = os.path.join(unmatched_authors_csv_subdir_path, unmatched_csv_name)

        # saving as CSV
        df_unmatched.to_csv(unmatched_csv_path, index=False)
        print(f"[i] df_unmatched successfully saved to {unmatched_csv_path}.")

    # if error_log is not an empty list, then we save the encountered errors to a CSV file
    if error_log:
        print(f"|!| Ran into a total of {len(error_log)} errors. Saving errors to CSV.")
        df_errors = pd.DataFrame(error_log)
        error_log_subdir = output_csv_subdir
        error_log_subdir_path = os.path.join(error_log_dir, error_log_subdir)
        os.makedirs(error_log_subdir_path, exist_ok=True)
        error_csv_path = os.path.join(error_log_subdir_path, f'{output_name}_error_log.csv')
        df_errors.to_csv(error_csv_path, index=False, encoding="utf-8")
        print(f"|!| Errors logged to {error_csv_path}")

    return df_ancient_authors_output, output_csv_path

### Step 3: Calling the function to retrieve QIDs, labels, aliases and writing languages associated with authors from the Trismegistos list

if __name__ == "__main__":
    retrieve_qids_aliases_lang_trismegistos_wikidata(INPUT_AUTHORS_LIST, SOURCE, SPARQL, OUTPUT_CSV_DIR, ERROR_LOG_DIR)