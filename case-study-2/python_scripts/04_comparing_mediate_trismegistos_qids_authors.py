#####------------------------------------------------------------------------Comparing MEDIATE and TRISMEGISTOS authors' lists and creating exclusive, intersection and union lists (CSV)-----------------------------------------------------------------######

### Step 0: Importing necessary libraries
import os
import pandas as pd
from datetime import datetime
import traceback

### Step 1: Defining relevant directories, file paths and variables

## 1.1. File paths to CSVs

TRISMEGISTOS_AUTHORS_CSV = r'path_to\use-case-2\output\authors_csv\03_trismegistos_ancient_authors_csv_wiki\03_last\03_20250914_trismegistos_ancient_authors_wiki_labelled.csv'
MEDIATE_AUTHORS_CSV = r'path_to\use-case-2\output\authors_csv\02_mediate_ancient_authors_csv_wiki\02_last\02_20250912_mediate_ancient_authors_wiki_labelled_last.csv'

## 1.2. Directories

OUTPUT_CSV_DIR = r'path_to\use-case-2\output\authors_csv'
ERROR_LOG_DIR = r'path_to\use-case-2\output\error_logs'

## 1.3. Other

INTERMEDIATE_TAG = '04_intermediate'
LAST_TAG = '04_last'

### Step 2: Defining the function(s)

## 2.1. : Defining the function to compare both CSVs, save lists of exclusive authors, intersection and union (new 'master', but incomplete (nb_items, nb_collections), CSV)

def comparing_mediate_and_trismegistos_authors(trismegistos_authors_csv, mediate_authors_csv, output_csv_dir, error_log_dir):
    """
Description:
Using previously obtained lists of MEDIATE and Trismegistos authors enriched with Wikidata information, compares both lists using QIDs in order to obtain lists of exclusive authors for both sources, 
as well as the intersection and the union of both lists.

Arguments:
trismegistos_authors_csv: list of canonical Trismegistos authors enriched with Wikidata QIDs, labels, aliases and writing languages (CSV).
mediate_authors_csv: list of MEDIATE ancient authors enriched with Wikidata QIDs, labels, aliases and writing languages (CSV).
output_csv_dir: directory where the 4 resulting CSVs should be saved.
error_log_dir: directory where the potentially encountered errors should be saved as CSV.

Returns:
df_exclusive_trismegistos: the DataFrame containing information regarding exclusive authors from the Trismegistos list (based on QID comparison)
df_exclusive_mediate: the DataFrame containing information regarding exclusive authors from the MEDIATE list (based on QID comparison) 
df_intersection: the DataFrame containing the informationg regarding authors comprising the intersection of both lists (based on QID comparison)
df_union: the DataFrame containing the information regarding authors comprising the union of both lists (hence, with incomplete information for some fields only available to authors from one or the other list (like nb_items and nb_collections))
"""


    # creating a timestamp for naming purposes
    timestamp = datetime.now().strftime('%Y%m%d')

    # creating an error_log to catch possible problems
    error_log = []

    # creating an all-encompassing 'try & error' structure
    try:

        # loading both CSV files as dataframes
        df_trismegistos = pd.read_csv(trismegistos_authors_csv)
        df_mediate = pd.read_csv(mediate_authors_csv)

        # normalising the q_identifier column datatype to ensure smooth comparison
        df_trismegistos["q_identifier"] = df_trismegistos["q_identifier"].astype(str).str.strip()
        df_mediate["q_identifier"] = df_mediate["q_identifier"].astype(str).str.strip()

        # checking for duplicate QIDs in Trismegistos list (by creating a df holding them)
        duplicate_qids_trismegistos = df_trismegistos[df_trismegistos.duplicated(subset=["q_identifier"], keep=False)]
        print(f"[!] Found {len(duplicate_qids_trismegistos)} duplicate q_identifiers in df_trismegistos:")
        print(duplicate_qids_trismegistos.sort_values("q_identifier"))

        # checking for duplicate QIDs in MEDIATE list (by creating a df holding them)
        duplicate_qids_mediate = df_mediate[df_mediate.duplicated(subset=["q_identifier"], keep=False)]
        print(f"[!] Found {len(duplicate_qids_mediate)} duplicate q_identifiers in df_mediate:")
        print(duplicate_qids_mediate.sort_values("q_identifier"))
        
        # saving the duplicates from each list in separate CSVs
        if len(duplicate_qids_trismegistos) > 0 or len(duplicate_qids_mediate) >0:
    
            # creating the appropriate subdirectories
            output_csv_subdir = f"04_comparing_mediate_and_trismegistos_ancient_authors_qids"
            duplicates_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, INTERMEDIATE_TAG)
            os.makedirs(duplicates_subdir_path, exist_ok=True)

            # saving MEDIATE and Trismegistos duplicate QIDs in separate CSV files
            if len(duplicate_qids_trismegistos) > 0: 
                # saving Trismegistos duplicates
                duplicates_trismegistos_path = os.path.join(duplicates_subdir_path, f"04_{timestamp}_duplicates_trismegistos_authors_qids.csv")
                duplicate_qids_trismegistos.to_csv(duplicates_trismegistos_path, index=False)
                print(f"[i] Successfully saved {len(duplicate_qids_trismegistos)} duplicates from the Trismegistos list based on QIDs in a separate CSV for later assessment. See: {duplicates_trismegistos_path}")

                # removing identified duplicates from df_trismegistos
                df_trismegistos = df_trismegistos[~df_trismegistos["q_identifier"].isin(duplicate_qids_trismegistos["q_identifier"])]
                print(f"[i] Successfully removed {len(duplicate_qids_trismegistos)} duplicates from df_trismegistos.")


            if len(duplicate_qids_mediate) > 0:
                # saving MEDIATE duplicates
                duplicates_mediate_path = os.path.join(duplicates_subdir_path, f"04_{timestamp}_duplicates_mediate_authors_qids.csv")
                duplicate_qids_mediate.to_csv(duplicates_mediate_path, index=False)
                print(f"[i] Successfully saved {len(duplicate_qids_mediate)} duplicates from the MEDIATE list based on QIDs in a separate CSV for later assessment. See: {duplicates_mediate_path}")

                # removing identified duplicates from df_mediate
                df_mediate = df_mediate[~df_mediate["q_identifier"].isin(duplicate_qids_mediate["q_identifier"])]
                print(f"[i] Successfully removed {len(duplicate_qids_mediate)} duplicates from df_mediate.")

        # checking that there are no duplicate QIDs within each df (redundant so commented out, but could be useful)
        # assert df_trismegistos["q_identifier"].is_unique
        # assert df_mediate["q_identifier"].is_unique

        # identifying exclusive rows for each dataframe
        df_exclusive_trismegistos = df_trismegistos[~df_trismegistos["q_identifier"].isin(df_mediate["q_identifier"])] # returns the "q_identifier" values from df_trismegistos that are not in df_mediate
        df_exclusive_mediate = df_mediate[~df_mediate["q_identifier"].isin(df_trismegistos["q_identifier"])] # returns the "q_identifier" values from df_mediate that are not in df_trismegistos

        # printing info statement
        print(f"[i] Found a total of {len(df_exclusive_trismegistos) + len(df_exclusive_mediate)} exclusive rows between both dfs. {len(df_exclusive_trismegistos)} are unique to df_trismegistos and {len(df_exclusive_mediate)} are unique to df_mediate.")

        # creating intersection
        # df_shared_mediate = df_mediate[df_mediate["q_identifier"].isin(df_trismegistos["q_identifier"])] # saves the intersection of "q_identifiers" but with the metadata from the df_mediate dataframe
        df_intersection = pd.merge(df_mediate, df_trismegistos, on="q_identifier", how="inner", suffixes=("_mediate", "_trismegistos")) # returns a df with all columns, hence when columns share the same name, "_mediate" and "_trismegistos" are added to differentiate them
        print(f"[i] df_mediate and df_trismegistos share a total of {len(df_intersection)} rows based on Q identifiers.")
        print(df_intersection.head())

        # reworking the columns of df_intersection so as to keep only values from MEDIATE columns when the two are available (otherwise a lot will be repeated, like labels, aliases and writing languages)
        df_intersection = df_intersection.loc[:, ~df_intersection.columns.str.endswith('_trismegistos')]
        df_intersection.columns = df_intersection.columns.str.replace('_mediate$', '', regex=True)
        print(f"[i] Reworked the names of the columns and deleted the useless ones.")
        print(df_intersection.shape)
        print(df_intersection.head())

        # creating union
        df_union = pd.merge(df_mediate, df_trismegistos, on="q_identifier", how="outer", suffixes=("_mediate", "_trismegistos"))
        print(f"[i] While df_trismegistos contained {len(df_trismegistos)} rows and df_mediate contained {len(df_mediate)} rows, df_union contains a total of {len(df_union)} unique rows (based on Q identifiers).")
        print(df_union.shape)
        print(df_union.head())

        # merging paired columns (_mediate and _trismegistos), prioritising mediate values if non-empty
        # defining which columns are mediate_columns
        mediate_columns = [column for column in df_union.columns if column.endswith('_mediate')]

        # then looping over these mediate_columns (which are the ones with a _trismegistos counterpart normally)
        for mediate_column in mediate_columns:
            root_column = mediate_column.replace('_mediate', '')
            trismegistos_column = root_column + '_trismegistos' # defining the _trismegistos counterpart to either delete it or use it if _mediate empty
            
            if trismegistos_column in df_union.columns: # this means if there is indeed a counterpart to the mediate_column
                # replacing empty strings with NaN for proper combine_first behavior
                df_union[mediate_column] = df_union[mediate_column].replace("", pd.NA)
                df_union[trismegistos_column] = df_union[trismegistos_column].replace("", pd.NA)

                # combining both columns, taking non-null values from mediate first (creating a separate column so that we can then just drop the others)
                df_union[root_column] = df_union[mediate_column].combine_first(df_union[trismegistos_column])
            else:
                # if no trismegistos counterpart, duplicating the mediate_column (this should not happen)
                df_union[root_column] = df_union[mediate_column]

        # now getting rid of all suffixed columns (both _mediate and _trismegistos)
        columns_to_drop = [column for column in df_union.columns if column.endswith('_mediate') or column.endswith('_trismegistos')]
        df_union.drop(columns=columns_to_drop, inplace=True)

        print(f"[i] Successfully combined paired _mediate and _trismegistos columns, priorising the values from _mediate (when non-null).")
        print(df_union.shape)
        print(df_union.head())

        # creating the subdirectory that will hold all the dataframes
        output_csv_subdir = f"04_comparing_mediate_and_trismegistos_ancient_authors_qids"
        clean_csv_subdir_path = os.path.join(output_csv_dir, output_csv_subdir, LAST_TAG)
        os.makedirs(clean_csv_subdir_path, exist_ok=True)

        # saving the exclusive_rows, the intersection and the union dfs
        # saving df_unique_trismegistos
        exclusive_trismegistos_csv_name = f"04_{timestamp}_exclusive_trismegistos_authors_qids.csv"
        exclusive_trismegistos_csv_path = os.path.join(clean_csv_subdir_path, exclusive_trismegistos_csv_name)
        df_exclusive_trismegistos.to_csv(exclusive_trismegistos_csv_path, index=False)

        # saving df_unique_mediate
        exclusive_mediate_csv_name = f"04_{timestamp}_exclusive_mediate_authors_qids.csv"
        exclusive_mediate_csv_path = os.path.join(clean_csv_subdir_path, exclusive_mediate_csv_name)
        df_exclusive_mediate.to_csv(exclusive_mediate_csv_path, index=False)  
        
        # saving df_intersection
        intersection_csv_name = f"04_{timestamp}_mediate_trismegistos_intersection_authors_qids.csv"
        intersection_csv_path = os.path.join(clean_csv_subdir_path, intersection_csv_name)
        df_intersection.to_csv(intersection_csv_path, index=False)

        # saving df_union
        union_csv_name = f"04_{timestamp}_mediate_trismegistos_union_authors_qids.csv"
        union_csv_path = os.path.join(clean_csv_subdir_path, union_csv_name)
        df_union.to_csv(union_csv_path, index=False)

    except Exception as e:
            error_log.append({
                "tm_id": "N/A",
                "error": str(e),
                "traceback": traceback.format_exc()
            })
    
    # if error_log is not an empty list, then we save the encountered errors to a CSV file
    if error_log:
        print(f"|!| Ran into a total of {len(error_log)} errors. Saving errors to CSV.")
        df_errors = pd.DataFrame(error_log)
        error_log_subdir = f"04_comparing_mediate_and_trismegistos_ancient_authors_qids"
        error_log_subdir_path = os.path.join(error_log_dir, error_log_subdir)
        os.makedirs(error_log_subdir_path, exist_ok=True)
        error_csv_path = os.path.join(error_log_subdir_path, f'04_{timestamp}_comparing_mediate_and_trismegistos_authors_qids_errors.csv')
        df_errors.to_csv(error_csv_path, index=False, encoding="utf-8")
        print(f"|!| Errors logged to {error_csv_path}")

    return df_exclusive_trismegistos, df_exclusive_mediate, df_intersection, df_union

### Step 3: Calling the function

if __name__ == "__main__":
    df_exclusive_trismegistos, df_exclusive_mediate, df_intersection, df_union = comparing_mediate_and_trismegistos_authors(
        TRISMEGISTOS_AUTHORS_CSV, MEDIATE_AUTHORS_CSV, OUTPUT_CSV_DIR, ERROR_LOG_DIR
    )