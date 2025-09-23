#####------------------------------------------------------------------------Retrieving relevant Wikidata information for authors from the MEDIATE 'cleaned results' list (CSV)-----------------------------------------------------------------######

### Step 0: Importing necessary libraries
import os
import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, JSON
from datetime import datetime
import traceback

### Step 1: Defining relevant directories, file paths and variables

INPUT_AUTHOR_LIST = r'path_to\use-case-2\input\initial_author_lists\mediate\csv\cleaned_results\ancient_authors_-900_500_mediate_cleaned_results.csv'
SOURCE = 'mediate'
OUTPUT_CSV_DIR = r'path_to\use-case-2\output\authors_csv'
ERROR_LOG_DIR = r'path_to\use-case-2\output\error_logs'
INTERMEDIATE_TAG = '02_intermediate'
LAST_TAG = '02_last'

## 1.3. SPARQL endpoint
SPARQL = SPARQLWrapper("https://query.wikidata.org/sparql",
                       agent="your_role - your_email@email.com")


### Step 2: Defining the function(s)

## 2.1. : Defining the function to retrieve information from Wikidata

# We are interested in  QIDs, French labels, English labels, French aliases, English aliases, Latin aliases and writing language(s)

def retrieve_qids_aliases_lang_wikidata(input_author_list, source, sparql_setup, output_csv_dir, error_log_dir, specific_ids=None, nb_ids=None):

    """
Queries Wikidata for authors in a MEDIATE 'cleaned results' CSV list using their VIAF cluster IDs (as reported on MEDIATE). The objective is to retrieve labels, aliases (in French, English and Latin),
and writing language(s). Results are then saved to a separate CSV. The function has some flexibility and can process part of the list or the full list.

Arguments:
    input_authors_list (str): Path to CSV with 'viaf_id' and 'short_name' columns (last output from 01_cleaning_mediate_results_xlsx).
    source (str): Source of the initial list: useful for naming (files and columns).
    sparql_setup: SPARQLWrapper setup with endpoint.
    output_directory (str): Path to the output directory used to save the CSVs.
    error_log_dir (str): Path to the directory to save error logs.
    specific_ids (list, optional): List of VIAF IDs to query. Defaults to None.
    nb_ids (int., optional): Limit number of authors to query. Defaults to None.

Returns:
    tuple: (DataFrame of results, path to output CSV)
"""
    # creating a timestamp for naming purposes
    timestamp = datetime.now().strftime('%Y%m%d')

    # initialising return objects to avoid crash
    df_ancient_authors_output = pd.DataFrame()  # empty dataframe as fallback
    output_csv_path = None
    df_not_matched_automatically = pd.DataFrame()
    
    # initialising a list to hold possible errors
    error_log = []

    try:
        # checking if the CSV containing the input_author_list exists
        if not os.path.exists(input_author_list):
            raise FileNotFoundError(f"|!| Error: input_author_list CSV not found: {input_author_list}")

        # loading the initial CSV containing the list of authors and making a copy
        df_authors_source = pd.read_csv(input_author_list)
        df_authors_input = df_authors_source.copy() # to avoid modifying anything we shouldn't in the original df

        ## (a) Preparing to query the Wikidata database
        # Deciding which authors to query: by default, querying names and aliases for all VIAF cluster IDs in the input_csv, but allowing for specific_ids or nb_ids

        if specific_ids is not None:
            author_ids = [str(id) for id in specific_ids if pd.notna(id)]
        else:
            if nb_ids is not None:
                author_ids = df_authors_input.iloc[:nb_ids,:]["viaf_id"].dropna().astype(str).tolist() # if some of the authors have a NaN viaf_id, then you will get less than nb_ids elements in the list.
            else:
                author_ids = df_authors_input["viaf_id"].dropna().astype(str).tolist() # .dropna() removes any NaN (missing) values from the Series (from 'viaf_id'column)

        # exiting early if no author_ids are found to query
        if not author_ids:
            print("|!| No author_ids found. Check strucutre of CSV or specific_ids = [].")
            return None

        # feeding the target author ids to the SPARQL query
        values_block = " ".join(f'"{id}"' for id in author_ids)

        # setting up the SPARQL query

        query = f"""
    
    SELECT ?viafID ?item ?itemLabelEN ?itemLabelFR ?itemLabelLA
        (GROUP_CONCAT(DISTINCT ?aliasEnglish; SEPARATOR=", ") AS ?aliasesEnglish)
        (GROUP_CONCAT(DISTINCT ?aliasFrench; SEPARATOR=", ") AS ?aliasesFrench)
        (GROUP_CONCAT(DISTINCT ?aliasLatin; SEPARATOR=", ") AS ?aliasesLatin)
        (GROUP_CONCAT(DISTINCT ?writingLangLabelEN; SEPARATOR=", ") AS ?writingLanguages)
    
    WHERE {{
    VALUES ?viafID {{ {values_block} }}
    ?item wdt:P214 ?viafID.

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
    GROUP BY ?viafID ?item ?itemLabelEN ?itemLabelFR ?itemLabelLA
    """

        # Calling the query
        sparql_setup.setQuery(query)
        sparql_setup.setReturnFormat(JSON)
        results = sparql_setup.query().convert()

        # quick check (commented out)
        # print(json.dumps(results, indent=2)) 

        ## (b) Processing the results of the SPARQL query on Wikidata and saving the results as a CSV

        # checking which viaf_ids were matched or not matched
        # collecting the matched viaf_ids
        matched_viaf_ids = {result["viafID"]["value"] for result in results["results"]["bindings"]}
        
        # keeping track of progress
        print(f"[i] The query matched {len(matched_viaf_ids)} VIAF cluster IDs from the MEDIATE 'cleaned_results' list of authors, out of {len(author_ids)}.")
        
        # creating a list of not atched viaf_ids
        not_matched_automatically_viaf_ids = [id_ for id_ in author_ids if id_ not in matched_viaf_ids]
        
        # keeping track of progress
        print(f"[i] The query was unable to match {len(not_matched_automatically_viaf_ids)} VIAF cluster IDs from the MEDIATE 'cleaned_results' list of authors, out of {len(author_ids)}.")
        print(f"[i] Will now create a DataFrame to hold the information retrieved from Wikidata for the matched VIAF cluster IDs.")

        # saving the data of the results as a list of dictionaries before creating the dataframe
        data = []

        for result in results["results"]["bindings"]:
            try:
                viaf_id = result["viafID"]["value"]

                # getting the main author labels in English, French and Latin (standard label)
                en_label = result.get("itemLabelEN", {}).get("value", "")
                fr_label = result.get("itemLabelFR", {}).get("value", "")
                la_label = result.get("itemLabelLA", {}).get("value", "")

                # Getting the QID for each queried author (to allow for cross-database comparison later)
                qid_uri = result.get("item", {}).get("value", "")
                qid = qid_uri.split("/")[-1] if qid_uri else None
                if not qid:
                    print(f"|!| No QID found for viaf_id: {viaf_id}") # safety check but likely useless

                # Getting the writing language(s) for every author
                writing_lang_labels = result.get("writingLanguages", {}).get("value", "").split(", ")
                writing_languages = list(set(filter(None, writing_lang_labels)))  # remove empty strings and duplicates (although unlikely), then transform back into list

                # Getting alternative author labels (aliases) and putting them in the right format for CSV storage
                aliases_en = result.get("aliasesEnglish", {}).get("value", "").split(", ")
                aliases_fr = result.get("aliasesFrench", {}).get("value", "").split(", ")
                aliases_la = result.get("aliasesLatin", {}).get("value", "").split(", ")

                # again, removing empty strings, duplicates and returning everything as lists
                aliases_en = list(set(filter(None, aliases_en)))
                aliases_fr = list(set(filter(None, aliases_fr)))
                aliases_la = list(set(filter(None, aliases_la)))

                # mapping matched viaf_id back to the initial df_authors_input to extract some information from there and use it in the last CSV
                source_row = df_authors_input.loc[df_authors_input["viaf_id"].astype(str) == str(viaf_id)] # returns a filtered 'DataFrame' with the row for which viaf_id value matches the viaf_id under consideration
                source_label = source_row["short_name"].iloc[0] if not source_row.empty else None
                viaf_id = source_row["viaf_id"].iloc[0] if not source_row.empty else None
                source_nb_items = source_row["nb_items"].iloc[0] if not source_row.empty else None
                source_nb_collections = source_row["nb_collections"].iloc[0] if not source_row.empty else None

                # Storing everything in the output data_dictionary for each author found
                data.append({
                    "english_label": en_label,
                    "french_label": fr_label,
                    "latin_label": la_label,
                    "q_identifier": qid,
                    f"{source}_label": source_label,
                    f"viaf_id": viaf_id,
                    f"{source}_nb_items": source_nb_items,
                    f"{source}_nb_collections": source_nb_collections,
                    "english_aliases": aliases_en,
                    "french_aliases": aliases_fr,
                    "latin_aliases": aliases_la,
                    "writing_languages": writing_languages,
                })

            # tracking errors inside the results loop
            except Exception as e:
                error_log.append({
                "viaf_id": result.get("viafID", {}).get("value", "N/A"),
                "error": str(e),
                "traceback": traceback.format_exc()
                })

        ## (c) Saving the results

        # creating a dataframe holding the results of the query for all queried author_ids that were matched
        df_ancient_authors_output = pd.DataFrame(data)

        # making sure that the aliases and the writing languages are correctly saved (JSON formatted strings when retrieving the query)
        df_ancient_authors_output["english_aliases"] = df_ancient_authors_output["english_aliases"].apply(json.dumps)
        df_ancient_authors_output["french_aliases"] = df_ancient_authors_output["french_aliases"].apply(json.dumps)
        df_ancient_authors_output["latin_aliases"] = df_ancient_authors_output["latin_aliases"].apply(json.dumps)

        df_ancient_authors_output["writing_languages"] = df_ancient_authors_output["writing_languages"].apply(json.dumps)

        # then don't forget to apply 
        # df = pd.read_csv("your_file.csv")
        # df["english_aliases"] = df["english_aliases"].apply(json.loads) - when you reload the CSV, otherwise will not be read as a list but as a string

        # printing overview of the results
        print(df_ancient_authors_output.head())

        # defining the subdirectory and the CSV file name
        output_csv_subdir = f'02_{source}_ancient_authors_csv_wiki'
        output_csv_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, INTERMEDIATE_TAG)
        os.makedirs(output_csv_subdir_path, exist_ok=True)

        # adjusting the name of the output CSV file depending on source and target authors  
        if specific_ids is None and nb_ids is None:
            output_name = f'02_{timestamp}_{source}_ancient_authors_wiki_labelled_first.csv'
        elif specific_ids: 
            match_row = df_authors_input.loc[df_authors_input["viaf_id"].astype(str) == str(specific_ids[0])] # use first VIAF cluster ID for naming (take from source DF because unsure whether we matched)
            author_label = match_row["short_name"].iloc[0] if not match_row.empty else "unknown" # should not happen that match_row is empty (unless error in the passed VIAF cluster ID)
            output_name = f'02_{timestamp}_{source}_ancient_authors_wiki_labelled_{author_label}_{len(author_ids)}.csv' # indicate the number of ids used but only write 1st
        elif nb_ids:
            output_name = f'02_{timestamp}_{source}_ancient_authors_wiki_labelled_{nb_ids}_nb_ids.csv'

        # defining the output path and saving the dataframe with the collected labels, QIDs, and writing languages as CSV
        output_csv_path = os.path.join(output_csv_subdir_path, output_name)
        df_ancient_authors_output.to_csv(output_csv_path, index=False)

        print(f"|Y| {output_name} saved to {output_csv_path}.\n >>>> :)")

    except Exception as e:
        error_log.append({
            "tm_id": "N/A",
            "error": str(e),
            "traceback": traceback.format_exc()
        })
    
    # now dealing with the not matched (automatically) viaf_ids from earlier
    if not_matched_automatically_viaf_ids:
        print(f"|!| Unable to automatically match {len(not_matched_automatically_viaf_ids)} author(s) from the list with their VIAF IDs. Check the CSV and update manually (or use the function defined in 2.2.).")

        # saving the rows to a CSV
        # creating a df holding those rows
        df_not_matched_automatically = df_authors_input[df_authors_input["viaf_id"].astype(str).isin(not_matched_automatically_viaf_ids)].copy()

        # creating the appropriate subdir, name and path
        output_csv_subdir = f'02_{source}_ancient_authors_csv_wiki'
        output_csv_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, INTERMEDIATE_TAG)
        os.makedirs(output_csv_subdir_path, exist_ok=True)

        not_matched_automatically_csv_name = f"02_{timestamp}_{source}_not_matched_automatically_ancient_authors_wiki_viaf_id.csv"
        not_matched_automatically_csv_path = os.path.join(output_csv_subdir_path, not_matched_automatically_csv_name)

        # saving as CSV
        df_not_matched_automatically.to_csv(not_matched_automatically_csv_path, index=False)
        print(f"[i] df_not_matched_automatically successfully saved to {not_matched_automatically_csv_path}.")

    # if error_log is not an empty list, then we save the encountered errors to a CSV file
    if error_log:
        print(f"|!| Ran into a total of {len(error_log)} errors. Saving errors to CSV.")
        df_errors = pd.DataFrame(error_log)
        error_log_subdir = f'02_{source}_ancient_authors_csv_wiki_error_logs'
        error_log_subdir_path = os.path.join(error_log_dir, error_log_subdir)
        os.makedirs(error_log_subdir_path, exist_ok=True)
        error_csv_path = os.path.join(error_log_subdir_path, f'02_{timestamp}_{source}_ancient_authors_wiki_error_log_{len(author_ids)}.csv')
        df_errors.to_csv(error_csv_path, index=False, encoding="utf-8")
        print(f"|!| Errors logged to {error_csv_path}")
    
    else:
        print(f"|Y| No errors detected when processing the list of authors. Done processing.")

    return df_ancient_authors_output, output_csv_path, df_not_matched_automatically

## 2.2.: Defining the function to deal with df_not_matched_automatically

def complete_unmatched_with_manual_viaf_interactive(df_matched, df_not_matched, source, sparql_setup, output_csv_dir, error_log_dir, timestamp=None):
    """
    Description:
    Prompts user interactively to input missing VIAF cluster IDs for unmatched authors.
    These should be searched for on Wikidata (for instance), manually.
    Then the function re-queries Wikidata for those authors and appends the new matches and their information
    to the original matched dataframe. Saves the combined result as a new CSV.

    Argumentss:
        df_matched (pd.DataFrame): DataFrame of already matched authors.
        df_not_matched (pd.DataFrame): DataFrame of authors not matched in query of previous run.
        source (str): Used for file naming and column tracking.
        sparql_setup: SPARQLWrapper setup.
        output_csv_dir (str): Where to save the CSVs.
        timestamp (str, optional): To reuse the same timestamp as previous function. Defaults to today's date.

    Returns:
        last_df: The last DataFrame comibining automatically and semi-automatically queried authors (with VIAF cluster IDs).
    """

    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d')

    # creating a list to hold the dictionaries containing the information regarding viaf_ids to be entered manually
    manual_entries = []

    # creating a list to hold the dictionaries containing information of skipped entries
    skipped_entries = []
    
    print(f"\n>>>> Proceeding to correct viaf_ids manually <<<<")
    print(f"[guidelines] For each unmatched author, you can paste a valid VIAF ID from Wikidata (or leave empty to skip).\n")

    for i, (_, row) in enumerate(df_not_matched.iterrows(), start=1):
        label = row.get("short_name", "N/A")
        dob = row.get("date_of_birth", "N/A")
        dod = row.get("date_of_death", "N/A")
        first_names = row.get("first_names", "N/A")
        surname = row.get("surname", "N/A")
        items = row.get("nb_items", "N/A")
        collections = row.get("nb_collections", "N/A")
        initial_id = row.get("viaf_id", "N/A")
        
        print(f"[{i}/{len(df_not_matched)}] Processing author {i}:")
        print(f"[{i}/{len(df_not_matched)}] Short name: {label}")
        print(f"[{i}/{len(df_not_matched)}] Full name: {first_names}, {surname}")
        print(f"[{i}/{len(df_not_matched)}] DOB: {dob} | DOD: {dod}")
        print(f"[{i}/{len(df_not_matched)}] Search on: https://www.wikidata.org/wiki/Special:Search?search={label.replace(' ', '+')}")
        
        viaf_id = input("[ACTION REQUIRED] Enter new VIAF ID (or press Enter to skip): ").strip()

        if viaf_id:
            manual_entries.append({
                "short_name": label,
                "new_viaf_id": viaf_id,
                "nb_items": items,
                "nb_collections": collections
            })
        else:
            skipped_entries.append({
                "short_name": label,
                "initial_viaf_id": initial_id,
                "first_names": first_names,
                "surname": surname,
                "date_of_birth": dob,
                "date_of_death": dod,
                "nb_items": items,
                "nb_collections": collections
            })

    if not manual_entries:
        print("|!| No VIAF IDs entered. Skipping re-query.")
        return df_matched, df_not_matched

    # saving skipped entries before proceeding to the re-query
    if skipped_entries:
        print(f"[i] Skipped {len(skipped_entries)} entries from the not_matched_automatically_csv. Saving those separately.")
        
        # creating the df
        df_skipped_not_matched_authors = pd.DataFrame(skipped_entries)

        # creating the appropriate subdir and path
        output_csv_subdir = f'02_{source}_ancient_authors_csv_wiki'
        output_csv_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, INTERMEDIATE_TAG)
        os.makedirs(output_csv_subdir_path, exist_ok=True)
        skipped_not_matched_authors_csv_path = os.path.join(output_csv_subdir_path, f"02_{timestamp}_{source}_skipped_authors_not_matched_viaf_ids.csv")
        
        # saving the df to CSV
        df_skipped_not_matched_authors.to_csv(skipped_not_matched_authors_csv_path, index=False)
        
        # keeping track of progress
        print(f"[i] Skipped authors (no VIAF entered) successfully saved to: {skipped_not_matched_authors_csv_path}.")

    # converting results to a dataframe to re-run the query
    print(f"[i] Will now create a DF to hold the results from the re-query of manually entered VIAF cluster IDs.")
    df_manually_entered_viaf_ids = pd.DataFrame(manual_entries)

    # running the initial SPARQL query on the updated entries
    output_csv_subdir = f'02_{source}_ancient_authors_csv_wiki'
    output_csv_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, INTERMEDIATE_TAG)
    os.makedirs(output_csv_subdir_path, exist_ok=True)
    output_manually_entered_viaf_ids_csv_path = os.path.join(output_csv_subdir_path, f"02_{timestamp}_{source}_authors_manually_added_viaf_ids.csv")
    df_manually_entered_viaf_ids.to_csv(output_manually_entered_viaf_ids_csv_path, index=False)

    print("[i] Querying Wikidata for manually entered VIAF IDs...\n")

    df_semi_manually_queried_authors, _, _ = retrieve_qids_aliases_lang_wikidata(
        input_author_list=output_manually_entered_viaf_ids_csv_path,
        source=source,
        sparql_setup=sparql_setup,
        output_csv_dir=output_csv_dir,
        error_log_dir=error_log_dir,
        specific_ids=df_manually_entered_viaf_ids["viaf_id"].tolist()
    )

    # combining the initially matched viaf_ids authors with the newly corrected ones
    last_df = pd.concat([df_matched, df_semi_manually_queried_authors], ignore_index=True)

    # saving the concatanated df as a CSV holding the updated mediate_ancient_authors_wiki_labelled_all
    # creating the appropriate subdir and path
    last_output_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, LAST_TAG)
    os.makedirs(last_output_subdir_path, exist_ok=True)
    last_output_name = f"02_{timestamp}_{source}_ancient_authors_wiki_labelled_last.csv"
    last_output_path = os.path.join(last_output_subdir_path, last_output_name)
    last_df.to_csv(last_output_path, index=False)

    print(f"|Y| Last combined results saved to: {last_output_path}. Total entries in last_df: {len(last_df)}.")

    return last_df


### Step 3: Calling both functions

if __name__ == "__main__":
    df_matched, _, df_not_matched = retrieve_qids_aliases_lang_wikidata(
        INPUT_AUTHOR_LIST, SOURCE, SPARQL, OUTPUT_CSV_DIR, ERROR_LOG_DIR
    )

    last_df = complete_unmatched_with_manual_viaf_interactive(
        df_matched=df_matched,
        df_not_matched=df_not_matched,
        source=SOURCE,
        sparql_setup=SPARQL,
        output_csv_dir=OUTPUT_CSV_DIR,
        error_log_dir=ERROR_LOG_DIR
    )
