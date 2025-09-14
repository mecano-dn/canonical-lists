#####----------------------------------------------------------------------------Cleaning the results from MEDIATE database (from XLSX to CSV)---------------------------------------------------------------------######

### Step 0: importing the necesary libraries

import pandas as pd
import os
import re

### Step 1: Defining relevant paths, directories and variables

## 1.1. Path to initial XLSX results from MEDIATE

MEDIATE_LISTS_DIR = r'path_to\use-case-2\input\initial_author_lists\mediate'
XLSX_SUBDIR = 'xlsx'
MEDIATE_XLSX_RAW_RESULTS_FILE_NAME = r'ancient_authors_-900_500_mediate.xlsx'
MEDIATE_XLSX_RAW_RESULTS_FILE_PATH = os.path.join(MEDIATE_LISTS_DIR, XLSX_SUBDIR, MEDIATE_XLSX_RAW_RESULTS_FILE_NAME)
CSV_SUBDIR = 'csv'
RAW_RESULTS_SUBDIR = 'raw_results'
CLEANED_RESULTS_SUBDIR = 'cleaned_results'


### Step 1: loading the XLSX as a dataframe and printing a preview to see if it works

df = pd.read_excel(MEDIATE_XLSX_RAW_RESULTS_FILE_PATH, na_values=['None']) # this will treat any 'None' values present in the XLSX as NaN
print(f"[i] MEDIATE raw results XLSX successfully loaded.\n")

print(df.head())

### Step 2: saving the original XLSX to a CSV

mediate_raw_results_csv_name_with_ext = os.path.basename(MEDIATE_XLSX_RAW_RESULTS_FILE_PATH)
mediate_raw_results_csv_name = f'{os.path.splitext(mediate_raw_results_csv_name_with_ext)[0]}_raw_results.csv'
mediate_raw_results_csv_subdir_path = os.path.join(MEDIATE_LISTS_DIR, CSV_SUBDIR, RAW_RESULTS_SUBDIR)
os.makedirs(mediate_raw_results_csv_subdir_path, exist_ok=True)
mediate_raw_results_csv_path = os.path.join(mediate_raw_results_csv_subdir_path, mediate_raw_results_csv_name)

df.to_csv(mediate_raw_results_csv_path, index=False)
print(f"[i] MEDIATE database raw results successfully saved to CSV in {os.path.join(MEDIATE_LISTS_DIR, CSV_SUBDIR, RAW_RESULTS_SUBDIR)}.\n")

### Step 3: loading the new CSV and cleaning it (naming columns, dropping unnecessary ones, etc.)

## 3.1. Loading the new CSV as df and dropping the index from the XLSX first downloaded from mediate

# reloading the initial mediate_raw_results_csv and making a copy to be cleaned
df_mediate_raw_results = pd.read_csv(mediate_raw_results_csv_path)
df_cleaned_mediate_raw_results = df_mediate_raw_results.copy()
print(f"[i] Here is an overview of the df_cleaned_mediate_raw_results after loading the CSV:\n")
print(df_cleaned_mediate_raw_results.head())

# dropping the first column which corresponds to the initial index as downloaded from MEDIATE
df_cleaned_mediate_raw_results = df_cleaned_mediate_raw_results.drop(columns='Unnamed: 0')
print(df_cleaned_mediate_raw_results.head())
print(f"[i] Successfully deleted column: 'Unnamed: 0 from df_cleaned_mediate_raw_results'")

## 3.2. Naming the columns and dropping the ones uninteresting to our project

# based on observations, renaming the columns of the CSV so that they become operable
df_cleaned_mediate_raw_results.columns = ['nb_items', 'nb_collections', 'short_name', 'first_names', 'surname', 'sex', 'city_of_birth', 'date_of_birth', 'city_of_death', 'date_of_death', 'collections', 'viaf_id', 'publisher_cerl', 'notes', 'bibliography', 'normalised_date_of_birth', 'normalised_date_of_death', 'earliest_edition_year', 'relations']
print(f"[i] Successfully renamed columns from df_cleaned_mediate_raw_results")
print(df_cleaned_mediate_raw_results.head())

# keeping only interesting columns
df_cleaned_mediate_raw_results = df_cleaned_mediate_raw_results[['short_name', 'nb_items', 'nb_collections', 'first_names', 'surname', 'date_of_birth', 'date_of_death', 'viaf_id']]
print(f"[i] First successful sorting of the columns of df_cleaned_mediate_raw_results.\n")
print(df_cleaned_mediate_raw_results.head())

## 3.3. Keeping only rows where VIAF cluster IDs are available and valid (numeric) to query Wikidata later. Extracting and saving duplicates apart.

# since we will need some sort of ID to (automatically) retrieve information from Wikidata and cross-analyse the contents comparing with the Trismegistos list, deleting rows where viaf_id is empty or NaN (but saving them to add them back manually later)
print(f"[i] Will now check viaf_id column for empty or NaN values.")

# first converting all empty viaf_id values into NaN
df_cleaned_mediate_raw_results['viaf_id'] = df_cleaned_mediate_raw_results['viaf_id'].replace('', pd.NA)

# identifying rows for which there is no viaf_id and saving them to CSV before dropping them
no_viaf_id_rows = df_cleaned_mediate_raw_results[df_cleaned_mediate_raw_results['viaf_id'].isna()]

if len(no_viaf_id_rows) > 0:
    print(f"[i] Found {len(no_viaf_id_rows)} rows that contained empty or NaN viaf_ids. Will now save these as separte CSV and then drop them.")
    
    print(no_viaf_id_rows.head())

    # creating the appropriate subdir and path to save the no_viaf_rows as a CSV
    dropped_subdir_name = 'dropped'
    dropped_subdir_path = os.path.join(MEDIATE_LISTS_DIR, CSV_SUBDIR, dropped_subdir_name)
    os.makedirs(dropped_subdir_path, exist_ok=True)
    no_viaf_id_csv_path = os.path.join(dropped_subdir_path, 'dropped_no_viaf_id.csv')

    # saving the df as CSV
    no_viaf_id_rows.to_csv(no_viaf_id_csv_path, index=False)
    print(f"[i] Saved {len(no_viaf_id_rows)} rows with missing VIAF cluster IDs to: {no_viaf_id_csv_path}")

    # now we drop the rows that contain NaN values in viaf_id column 
    before = len(df_cleaned_mediate_raw_results)
    df_cleaned_mediate_raw_results = df_cleaned_mediate_raw_results.dropna(subset=['viaf_id'])
    after = len(df_cleaned_mediate_raw_results)
    print(f"[i] Successfully deleted {before-after} rows in the dataframe where viaf_id was empty. DF has now {after} rows. Now extracting the id from the url.")

    # then reworking this column to only keep the viaf_id as an integer (which will be used to query Wikidata)
    df_cleaned_mediate_raw_results['viaf_id'] = df_cleaned_mediate_raw_results['viaf_id'].str.extract(r'/(\d+)/?$')
    print(f"[i] Successfully extracted the viaf_id(s) for all non-empty rows.\n")

else:
    print(f"[i] No empty viaf_id cells found.")

print(df_cleaned_mediate_raw_results.head())

print(f"[i] Will now proceed to filtering DOB and DOD.")

## 3.4. Formatting the DOB and DOD columns to filter errors (i.e. authors with DOB > 500 CE or DOD > 600 CE)

# parsing the 'date_of_birth' and 'date_of_death' columns to eliminate the rows where DOB > 500 AD or DOD > 600 CE

# first transforming DOB and DOD into strings (if not empty)
df_cleaned_mediate_raw_results['date_of_birth'] = df_cleaned_mediate_raw_results['date_of_birth'].where(
    df_cleaned_mediate_raw_results['date_of_birth'].isnull(), 
    df_cleaned_mediate_raw_results['date_of_birth'].astype(str)
    )

df_cleaned_mediate_raw_results['date_of_death'] = df_cleaned_mediate_raw_results['date_of_death'].where(
    df_cleaned_mediate_raw_results['date_of_death'].isnull(), 
    df_cleaned_mediate_raw_results['date_of_death'].astype(str)
    )
print(f"[i] Successfully transformed DOB and DOD into strings.")

# the .where(condition, other) is a pandas Series method that: keeps the original value where the condition is True, and replaces with other where the condition is False
# here if the value of the row in 'date_of_birth' column is not None (NaN), it will turn it into a string

# checking how many rows we have in the table before the process of dropping the ones with DOB > 500 CE or DOD > 600 CE
before = len(df_cleaned_mediate_raw_results)

# now looking to throw out rows where DOB > 500 CE or DOD > 600 CE
# Regex pattern to match DD-MM-YYYY (strict)
pattern = r'^(\d{2})-(\d{2})-(\d{4})$'

# defining the function that will check whether the row DOB is posterior to 500 CE or the row DOD is posterior to 600 CE 
def check_dob_and_dod(row):
    dob = row['date_of_birth']
    dod = row['date_of_death']

    dob_post_500 = False
    dod_post_600 = False

    if isinstance(dob, str):
        match = re.match(pattern, dob)
        if match:
            day, month, year = match.groups()
            if int(year) > 500:
                dob_post_500 = True

    if isinstance(dod, str):
        match = re.match(pattern, dod)
        if match:
            day, month, year = match.groups()
            if int(year) > 600:
                dod_post_600 = True

    return dob_post_500 or dod_post_600

# creating a mask to identify rows where DOB is DD-MM-YYYY and year > 500 CE or DOD is DD-MM-YYYY and year > 600 CE

# first removing the trailing white spaces to make sure we find all DD-MM-YYYY formats available
df_cleaned_mediate_raw_results['date_of_birth'] = df_cleaned_mediate_raw_results['date_of_birth'].str.strip() # just cleaning trailing whitespace
df_cleaned_mediate_raw_results['date_of_death'] = df_cleaned_mediate_raw_results['date_of_death'].str.strip()

# creating the mask to apply the check_dob_and_dod function
mask = df_cleaned_mediate_raw_results.apply(check_dob_and_dod, axis=1)

# printing the rows where the DOB > 500 CE or DOD > 600 CE
print(f"[i] Found {mask.sum()} rows where DOB is DD-MM-YYYY and > 500 CE or DOD is DD-MM-YYYY and > 600 CE.\n")
print(df_cleaned_mediate_raw_results[mask])

# saving those to-be-dropped rows to a CSV (for review)
dropped_rows = df_cleaned_mediate_raw_results[mask]

if mask.sum() > 0:
    dropped_subdir_name = 'dropped'
    dropped_subdir_path = os.path.join(MEDIATE_LISTS_DIR, CSV_SUBDIR, dropped_subdir_name)
    os.makedirs(dropped_subdir_path, exist_ok=True)
    dropped_rows_csv_path = os.path.join(dropped_subdir_path, 'dropped_dob_post_500_dod_post_600.csv')
    dropped_rows.to_csv(dropped_rows_csv_path, index=False)

    print(f"[i] Now dropping those rows.")

    # dropping rows where DOB is DD-MM-YYYY and year > 500 CE or DOD is DD-MM-YYY and > 600 CE
    df_cleaned_mediate_raw_results = df_cleaned_mediate_raw_results[~mask].copy()
    df_cleaned_mediate_raw_results.reset_index(drop=True, inplace=True)

    # checking how many rows we have left after this first filtering by DOB and DOD
    after = len(df_cleaned_mediate_raw_results)
    print(f"[i] Successfully dropped {before - after} rows from the dataframe.\n")

    print(df_cleaned_mediate_raw_results.head())
else:
    print("[i] No need to save empty dropped rows (none).")

# keeping track of next step
print("[i] Will now format the nb_items and nb_collections columns to integers.")

## 3.5. Formatting the nb_items and nb_collections columns to integers (0 is None or '')

# making sure the nb_items and nb_collections columns contain integers
# converting to numeric (invalid entries become Nan)
df_cleaned_mediate_raw_results['nb_items'] = pd.to_numeric(df_cleaned_mediate_raw_results['nb_items'], errors='coerce')
df_cleaned_mediate_raw_results['nb_collections'] = pd.to_numeric(df_cleaned_mediate_raw_results['nb_collections'], errors='coerce')

# then replacing possible NaNs with 0s and converting all numeric values to integers
df_cleaned_mediate_raw_results['nb_items'] = df_cleaned_mediate_raw_results['nb_items'].fillna(0).astype(int)
df_cleaned_mediate_raw_results['nb_collections'] = df_cleaned_mediate_raw_results['nb_collections'].fillna(0).astype(int)

# printing to keep track of progress
print("[i] Successfully formatted nb_items and nb_collections to integers.")

## 3.6. Saving the cleaned MEDIATE 'raw' results

# getting an overview of the table after these first filtering steps
print(f"[i] The final df now contains {len(df_cleaned_mediate_raw_results)} rows (from 245).")
print(df_cleaned_mediate_raw_results.head())

# some printing to keep track of progress
print(f"[i] Saving the final cleaned_mediate_raw_results as {os.path.splitext(mediate_raw_results_csv_name_with_ext)[0]}_cleaned_results.csv.")

# saving the CSV
# creating the appropriate subdir and path
mediate_cleaned_results_csv_name = f'{os.path.splitext(mediate_raw_results_csv_name_with_ext)[0]}_cleaned_results.csv'
mediate_cleaned_results_csv_subdir_path = os.path.join(MEDIATE_LISTS_DIR, CSV_SUBDIR, CLEANED_RESULTS_SUBDIR)
os.makedirs(mediate_cleaned_results_csv_subdir_path, exist_ok=True)
mediate_cleaned_results_csv_path = os.path.join(mediate_cleaned_results_csv_subdir_path, mediate_cleaned_results_csv_name)

# final save
df_cleaned_mediate_raw_results.to_csv(mediate_cleaned_results_csv_path, index=False)
print(f"[i] Successfully saved the df_cleaned_mediate_raw_results as CSV: {mediate_cleaned_results_csv_path}.")