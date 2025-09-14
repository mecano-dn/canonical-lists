#####------------------------------------------------------------------------Manually adding authors to the main mediate_ancient_authors.csv (CSV)-----------------------------------------------------------------######

### Step 0: Importing necessary libraries
import os
import pandas as pd
import json
from datetime import datetime

### Step 1: Defining important file paths, directories and variables

## 1.1.: File paths

INPUT_MEDIATE_ANCIENT_AUTHORS_CSV = r'path_to\use-case-2\output\authors_csv\05_matching_exclusive_trismegistos_authors_to_mediate_authors\05_last\05_20250914_concatenated_mediate_ancient_authors.csv'

## 1.2.: Directories

OUTPUT_CSV_DIR = r'path_to\use-case-2\output\authors_csv'

## 1.3.: Other

INTERMEDIATE_TAG = '06_intermediate'
LAST_TAG = '06_last'

### Step 2: Defining the new rows we want to add manually as dictionaries and storing them in a list

### List of remaining unmatched authors from the exclusive set of Trismegistos authors

# (index) Trismegistos label (QID)                  | Found manually in the MEDIATE JSON table |
# --------------------------------------------------|------------------------------------------|
# (1) Paulinus of Nola (Q132473)                    | No                                
# (2) Isidorus of Sevilla (Q166876)                 | Yes
# (3) Donatus (Q247137)                             | No
# (4) Gaius (Q313439)                               | No
# (5) Fronto (Q317055)                              | No
# (6) Servius (Q355350)                             | No
# (7) Priscianus (Q356433)                          | Yes
# (8) Rufinus of Aquileia (Q365835)                 | No
# (9) Blossius Aemilius Dracontius (Q885446)        | Yes
# (10) Scriptores Historiae Augustae (Q9334638)     | No
# (11) Augustus (Q1405)                             | No
# (12) Seneca (Q2054)                               | Yes
# (13) Gaius Cornelius Gallus (Q8825)               | No
# (14) Anonymi of the Corpus Hermeticum (Q192358)   | Yes
# (15) Kallimachos (Q192417)                        | Yes
# (16) Archilochos (Q201323)                        | No
# (17) Chrysippos (Q211411)                         | No
# (18) Alkaios (Q212872)                            | No
# (19) Simonides of Keos (Q273003)                  | No
# (20) Alkman (Q298850)                             | No
# (21) Bakchylides (Q310681)                        | No
# (22) Iamblichos of Chalkis (Q310916)              | Yes
# (23) Nonnos of Panopolis (Q312916)                | Yes
# (24) Heliodoros of Emesa (Q313011)                | Yes
# (25) Anaxagoras (Q83041)                          | No
# (26) Areios (Q106026)                             | No
# (27) Xenophanes of Kolophon (Q131671)             | No
# (28) Solon (Q133337)                              | No
# (29) Protagoras Abderita (Q169243)                | No
# (30) Poseidonios of Apameia (Q185770)             | No
# (31) Tatianos (Q272087)                           | No
# (32) Ioannes Philoponos (Q317632)                 | No 
# (33) Herakleitos of Ephesos (Q41155)              | No
# (34) Clemens of Rome (Q42887)                     | Yes
# (35) Athanasios (Q44024)                          | Yes
# (36) Stesichoros (Q332797)                        | No
# (37) Herondas (Q434811)                           | No
# (38) Euagrios of Pontos (Q437869)                 | No
# (39) Philodemos (Q451550)                         | No
# (40) Pseudo-Longinos or Pseudo-Dionysios (Q744540)| No
# (41) Poseidippos of Pella (Q1392801)              | No
# (42) Venantius Fortunatus (Q44934)                | No
# (43) Gorgias of Leontinoi (Q179785)               | No
# (44) Pelagius (Q162593)                           | No
# (45) Mani (Q203922)                               | No
# (46) Hippolytos of Rome (Q207113)                 | No
# (47) Hermas (Q1799472)                            | No
# (48) Sokrates (Q913)                              | No
# (49) Pythagoras (Q10261)                          | No

### List of added authors

# Isidore of Seville (Q166876)
# Priscian (Q356433)
# Seneca (Q2054)
# Hermes Trismegistus (Q192358)
# Callimachus of Cyrene (Q192417)
# Iamblichus (Q310916)
# Nonnus of Panopolis (Q312916)
# Heliodorus (of Emesa) (Q313011)
# Clement I, Pope (saint) (Q42887)
# Athanasius of Alexandria (Q44024) 



## 2.1. Defining the new rows to add to the 'master' MEDIATE ancient authors CSV

# Template
#   new_row = {
#       'english_label': 'New Author',                              -- from Wikidata (or list of exclusive Trismegistos authors)
#       'french_label': 'Nouvel Auteur',                            -- from Wikidata (or list of exclusive Trismegistos authors)
#       'latin_label': 'Novus Auctor',                              -- from Wikidata (or list of exclusive Trismegistos authors)
#       'q_identifier': 'Q999999',                                  -- from Wikidata (or list of exclusive Trismegistos authors)
#       'mediate_label': 'Author, New',                             -- from Wikidata (or list of exclusive Trismegistos authors)
#       'viaf_id': 123456789,                                       -- from MEDIATE JSON table
#       'mediate_nb_items': 10,                                     -- from MEDIATE JSON table
#       'mediate_nb_collections': 5,                                -- from MEDIATE JSON table
#       'english_aliases': ['New Author', 'Author New'],            -- from Wikidata (or list of exclusive Trismegistos authors)
#       'french_aliases': ['Nouvel Auteur', 'Auteur Nouveau'],      -- from Wikidata (or list of exclusive Trismegistos authors)
#       'latin_aliases': ['Novus Auctor', 'Auctor Novus'],          -- from Wikidata (or list of exclusive Trismegistos authors)
#       'writing_languages': ['Ancient Greek', 'Latin']             -- from Wikidata (or list of exclusive Trismegistos authors)
#   }

author1 ={
    'english_label': 'Isidore of Seville',
    'french_label': 'Isidore de Séville',
    'latin_label': 'Isidorus Hispalensis',
    'q_identifier': 'Q166876',
    'mediate_label': 'Isidore of Seville (saint)',
    'viaf_id': 803890, # from Wikidata (as retrieved in exclusive_trismegistos_authors)
    'mediate_nb_items': 16,
    'mediate_nb_collections': 15,
    'english_aliases': ['St. Isidore', 'Bishop of Seville', 'Isidorus Hispalensis', 'Isidor of Seville', 'Saint Isidore of Seville'],
    'french_aliases': ['Isidoire de Séville', 'Saint Isidore de Séville', 'Isidore de Seville'],
    'latin_aliases': ['Laus Hispaniae Isidori Hispaliensis', 'Isidorus'],
    'writing_languages': ['Latin']
}

author2 = {
    'english_label': 'Priscian',
    'french_label': 'Priscien de Césarée',
    'latin_label': 'Priscianus Caesariensis',
    'q_identifier': 'Q356433',
    'mediate_label': 'Priscianus Caesariensis',
    'viaf_id': 76294069, # from Wikidata (using the first one)
    'mediate_nb_items': 20,
    'mediate_nb_collections': 20,
    'english_aliases': ['Priscianus', 'Priscian of Caesarea'],
    'french_aliases': ['Priscien'],
    'latin_aliases': ['Priscianus', 'Priscianus Caesarensis', 'Priscianus Grammaticus'],
    'writing_languages': ['Latin']
}

author3 = {
    'english_label': 'Seneca',
    'french_label': 'Sénèque',
    'latin_label': 'Lucius Annaeus Seneca minor',
    'q_identifier': 'Q2054',
    'mediate_label': 'Seneca, Lucius Annaeus',
    'viaf_id': 60158790620538851262, # from Wikidata
    'mediate_nb_items': 994,
    'mediate_nb_collections': 387,
    'english_aliases': ['Lucius Annaeus Seneca', 'L. Annæus Seneca', 'Annaeus Seneca', 'Lucio Anneo Seneca', 'Lucius Annaeus Seneca Iunior', 'Seneca the Younger', 'Lucius Annaeus Seneca minor', 'the Younger Seneca', 'Lucius Annaeus Seneca the Younger'],
    'french_aliases': ['Lucius Annaeus Seneca', 'Sénèque le Jeune', 'Sénèque le Philosophe'],
    'latin_aliases': ['Lucius Annaeus Seneca', 'Cordubensis', 'L. Annaeus Seneca'],
    'writing_languages': ['Latin']
}

author4 = {
    'english_label': 'Hermes Trismegistus',
    'french_label': 'Hermès Trismégiste',
    'latin_label': 'Hermes Trismegistus',
    'q_identifier': 'Q192358',
    'mediate_label': 'Hermes Trismegistus',
    'viaf_id': 24571510, # from Wikidata
    'mediate_nb_items': 53,
    'mediate_nb_collections': 39,
    'english_aliases': ['Trismegistus'],
    'french_aliases': ['Hermes Trismegistus', 'Hermès Trismegiste', 'Hermes Trismegiste'],
    'latin_aliases': ['Mercurius ter Maximus'],
    'writing_languages': ['Ancient Greek']
}

author5 = {
    'english_label': 'Callimachus',
    'french_label': 'Callimaque de Cyrène',
    'latin_label': 'Callimachus',
    'q_identifier': 'Q192417',
    'mediate_label': 'Callimachus of Cyrene',
    'viaf_id': 99454635, # from Wikidata (with most references)
    'mediate_nb_items': 158,
    'mediate_nb_collections': 102,
    'english_aliases': ['Kallimachos of Kyrene', 'Battiades', 'Callimachus of Cyrene'],
    'french_aliases': ['Callimaque', 'Callimaque de Cyrene'],
    'latin_aliases': ['Callimachus Philosophus', 'Callimachus Cyrenaicus', 'Callimachus Poeta', 'Callimachus Cyrenaeus', 'Callimachus Cyrenensis'],
    'writing_languages': ['Ancient Greek']
}

author6 = {
    'english_label': 'Iamblichus',
    'french_label': 'Jamblique',
    'latin_label': 'Iamblichus',
    'q_identifier': 'Q310916',
    'mediate_label': 'Iamblichus',
    'viaf_id': 12310254, # from Wikidata (with most references)
    'mediate_nb_items': 121,
    'mediate_nb_collections': 85,
    'english_aliases': ['Iamblichus of Chalcis'],
    'french_aliases': ['Iamblichus', 'Jamblique de Chalcis', 'Jamblique de Tyr'],
    'latin_aliases': ['Abammon Syrus', 'Iamblichus Philosophus', 'Iamblichus Chalcidensis'],
    'writing_languages': ['Ancient Greek']
}

author7 = {
    'english_label': 'Nonnus of Panopolis',
    'french_label': 'Nonnos de Panopolis',
    'latin_label': 'Nonnus',
    'q_identifier': 'Q312916',
    'mediate_label': 'Nonnus of Panopolis',
    'viaf_id': 34461071, # from Wikidata (with most references)
    'mediate_nb_items': 54,
    'mediate_nb_collections': 40,
    'english_aliases': ['Nonnus'],
    'french_aliases': [],
    'latin_aliases': ['Nonnus Panopolitanus', 'Nonnus Epicus'],
    'writing_languages': ['Ancient Greek']
}

author8 = {
    'english_label': 'Heliodorus of Emesa',
    'french_label': "Héliodore d'Émèse",
    'latin_label': 'Heliodorus Emesenus',
    'q_identifier': 'Q313011',
    'mediate_label': 'Heliodorus (of Emesa)',
    'viaf_id': 62748316, # from Wikidata (with most references)
    'mediate_nb_items': 155,
    'mediate_nb_collections': 133,
    'english_aliases': ['Heliodorus'],
    'french_aliases': ["Heliodore d'Emese", 'Héliodore'],
    'latin_aliases': ['Heliodorus Scriptor eroticus', 'Heliodorus'],
    'writing_languages': ['Ancient Greek']
}

author9 = {
    'english_label': 'Clement I',
    'french_label': 'Clément de Rome',
    'latin_label': 'Clemens I',
    'q_identifier': 'Q42887',
    'mediate_label': 'Clement I, Pope (saint)',
    'viaf_id': 46860095, # from Wikidata (as retrieved in exclusive_trismegistos_authors)
    'mediate_nb_items': 8,
    'mediate_nb_collections': 8,
    'english_aliases': ['Saint Clemens', 'Clement the Bishop', 'St. Clement', 'St. Clemens', 'Saint Clement', 'Clemens', 'Clemens Romanus', 'Pope Saint Clement I', 'San Clemente', 'Pope Klyment I', 'Clemens the Bishop', 'Clement', 'Clemens I', 'Pope Klemens I', 'Saint Clement I', 'Clement of Rome', 'Flavius Clemens', 'Pope Kelemen I', 'Pope Clement I'],
    'french_aliases': ['Clement Ier', 'Clément Romain', 'Clément 1er', 'Saint Clement', 'Clément Ier'],
    'latin_aliases': ['Sanctus Clemens', 'Sanctus Clemens PP. I', 'Papa Clemens I', 'Clemens Romanus', 'Sanctus Clemens I'],
    'writing_languages': ['Ancient Greek']
}


author10 = {
    'english_label': 'Athanasius of Alexandria',
    'french_label': "Athanase d'Alexandrie",
    'latin_label': 'Athanasius of Alexandria',
    'q_identifier': 'Q44024',
    'mediate_label': 'Athanasius of Alexandria',
    'viaf_id': 105155222, # from Wikidata (using the first one)
    'mediate_nb_items': 77,
    'mediate_nb_collections': 63,
    'english_aliases': ['Athanasius the Confessor', "Sint Atanaze d'Alegzandreye", 'Athanasius Alexandrinus', 'Athanasius', 'Athanasius the Apostolic', 'Athanasius the Great', 'Athanasios of Alexandria', 'Saint Athanasius', 'Athanasius Contra Mundum', 'Athanasius I of Alexandria'],
    'french_aliases': ["Saint Athanase d'Alexandrie", 'Athanase le Grand', 'Athanase Ier', 'Athanasien', 'Athanase d’Alexandrie'],
    'latin_aliases':['Athanasius Magnus'],
    'writing_languages': ['Ancient Greek']
}

## 2.2. Creating the list of dictionaries to be passed as variable

LIST_AUTHORS_AS_DICTS = [author1, author2, author3, author4, author5, author6, author7, author8, author9, author10]

### Step 3: Creating a function to add manually entered rows
def manually_adding_authors_to_mediate_ancient_authors_csv(input_mediate_ancient_authors_csv, output_csv_dir, list_authors_as_dicts, sort=True):
    """
Description:
This function creates a new DataFrame from the list of manually-added dictionaries containing the authors to be added to the main mediate_ancient_authors.csv.
It then concatenates it with the DataFrame holding the previously arrived at mediate_ancient_authors.csv (automatically) and saves both the DataFrame of manually added entries and the updated_mediate_ancient_authors.csv.
It also scans for duplicates before saving the final updated_mediate_ancient_authors.csv.

Arguments:
input_mediate_ancient_authors_csv (str): path to the concatenated mediate_ancient_authors.csv from previous step (05) in the process
output_csv_dir (str): path to the output directory where to save the CSVs containing the lists of authors
list_authors_as_dicts (list): a list containing the authors to be added to the main mediate_ancient_authors.csv as dictionaries holding relevant information as fields
sort=True: default parameter to sort the concatenated DFS (the input_mediate_ancient_authors_csv and the manually added authors) according to 'mediate_nb_collections'

Returns:
updated_mediate_ancient_authors_csv_path (str): path to the last (final?) CSV containing all relevant MEDIATE ancient authors based on those initially found on the database with DOB filters, and the ones retrieved thanks to comparison with Trismegistos list
    
"""
    
    # creating a timestamp for naming purposes
    timestamp = datetime.now().strftime('%Y%m%d')

    # loading mediate_ancient_authors_csv
    print(f"[i] [loading] Loading input_authors_csv as df_existing_authors.")
    df_input_mediate_ancient_authors = pd.read_csv(input_mediate_ancient_authors_csv)

    # creating the df_updated_mediate_ancient_authors that will hold the authors to be added to the existing ones
    print(f"[i] [creating_df] Creating df_new_authors with the list of new authors passed as dictionaries.")
    df_added_authors = pd.DataFrame(list_authors_as_dicts)

    # converting the necessary rows (containing Python lists) to JSON strings before saving (to avoid problems)
    print(f"[i] [converting] Converting necessary columns of the df_added_authors to JSON strings to avoid errors with Python lists when saving as CSV. Remember to use json.loads when parsing next time.")
    df_added_authors ['english_aliases'] = df_added_authors['english_aliases'].apply(json.dumps)
    df_added_authors['french_aliases'] = df_added_authors['french_aliases'].apply(json.dumps)
    df_added_authors['latin_aliases'] = df_added_authors['latin_aliases'].apply(json.dumps)
    df_added_authors['writing_languages'] = df_added_authors['writing_languages'].apply(json.dumps)

    # getting an overview (quality check) of df_new_authors
    print(f"[i] [overview] Here is an overview of the df_added_authors holding the information concerning authors to be added to the input_mediate_ancient_authors_csv: \n")
    print(df_added_authors.shape)
    print(df_added_authors.head())

    # saving df_new_authors as a separate CSV
    # creating the appropriate subdirectory and naming file
    output_csv_subdir_name = '06_manually_adding_authors_to_final_mediate_csv'
    output_csv_subdir_path = os.path.join(output_csv_dir, output_csv_subdir_name)
    os.makedirs(output_csv_subdir_path, exist_ok=True)

    manually_added_authors_csv_subdir_path = os.path.join(output_csv_subdir_path, INTERMEDIATE_TAG)
    os.makedirs(manually_added_authors_csv_subdir_path, exist_ok=True)

    manually_added_authors_csv_name = f'06_{timestamp}_manually_added_authors_to_updated_mediate_ancient_authors.csv'
    manually_added_authors_csv_path = os.path.join(manually_added_authors_csv_subdir_path, manually_added_authors_csv_name)

    # saving the df as CSV
    df_added_authors.to_csv(manually_added_authors_csv_path, index=False)

    # concatenating both dfs
    print(f"[i] [concatenating] Concatenating both dfs as df_updated_mediate_ancient_authors.")
    df_updated_mediate_ancient_authors = pd.concat([df_input_mediate_ancient_authors, df_added_authors], ignore_index=True)

    # checking for duplicates before we proceed to sorting and saving (in case of manual error)
    df_duplicate_authors = df_updated_mediate_ancient_authors[df_updated_mediate_ancient_authors.duplicated(subset=["q_identifier"], keep='first')] # considering the first occurence as 'not a duplicate'
    print(f"[!] [duplicates] Found {len(df_duplicate_authors)} duplicate q_identifiers in df_updated_mediate_ancient_authors (based on QID comparison):")
    print(df_duplicate_authors.sort_values("q_identifier"))

    # if duplicates found, dropping them from df_updated_mediate_ancient_authors, then saving the second and later duplicate occurences as a separate CSV
    if len(df_duplicate_authors) > 0:
        # droping the duplicates from the df_updated_mediate_ancient_authors
        df_updated_mediate_ancient_authors = df_updated_mediate_ancient_authors.drop_duplicates(subset=["q_identifier"], keep='first')

        # creating appropriate subdirectory for df_duplicate_authors
        duplicate_authors_csv_subdir_path = os.path.join(output_csv_subdir_path, INTERMEDIATE_TAG)
        os.makedirs(duplicate_authors_csv_subdir_path, exist_ok=True)

        # naming the CSV that will hold the df_duplicate_authors
        duplicate_authors_csv_name = f'06_{timestamp}_duplicates_from_updated_mediate_ancient_authors.csv'
        duplicate_authors_csv_path = os.path.join(duplicate_authors_csv_subdir_path, duplicate_authors_csv_name)

        # saving the df_duplicate_authors to CSV
        df_duplicate_authors.to_csv(duplicate_authors_csv_path, index=False)
        print(f"[i] [duplicates] Successfully saved df_duplicate_authors to csv: {duplicate_authors_csv_path} ")
    
    # sorting df_updated_mediate_ancient_authors by nb_collections if sort=True
    if sort:
        print(f"[i] [sorting] Sorting df_updated_mediate_ancient_authors by 'mediate_nb_collections'. See overview:\n")
        df_updated_mediate_ancient_authors = df_updated_mediate_ancient_authors.sort_values(by='mediate_nb_collections', ascending=False).reset_index(drop=True)
        print(df_updated_mediate_ancient_authors.head())
    else:
        print((f"[i] [NOT sorting] Sorting was not required for df_updated_mediate_ancient_authors (see function parameters). Proceeding to save df to CSV."))

    # getting an overview of the df_updated_mediate_ancient_authors after removing duplicates and sorting
    print(f"[i] [overview] Printing an overview of the concatenated df_updated_mediate_ancient_authors after removing duplicates and sorting (if elected): \n")
    print(df_updated_mediate_ancient_authors.shape)
    print(df_updated_mediate_ancient_authors.head())

    # saving df_updated_mediate_ancient_authors to CSV
    print(f"[i] [saving] Saving df_updated_mediate_ancient_authors to CSV.")

    # creating appropriate directory and file name
    last_csv_subdir_path = os.path.join(output_csv_subdir_path, LAST_TAG)
    os.makedirs(last_csv_subdir_path, exist_ok=True)

    updated_mediate_ancient_authors_csv_name = f'06_{timestamp}_updated_mediate_ancient_authors.csv'
    updated_mediate_ancient_authors_csv_path = os.path.join(last_csv_subdir_path, updated_mediate_ancient_authors_csv_name)

    # saving df_updated_mediate_ancient_authors
    df_updated_mediate_ancient_authors.to_csv(updated_mediate_ancient_authors_csv_path, index=False)

    print(f"[i] Successfully saved the updated_mediate_ancient_authors.csv to: {updated_mediate_ancient_authors_csv_path}.")

    return updated_mediate_ancient_authors_csv_path

### Step 4: Calling the function
if __name__ == '__main__':
    manually_adding_authors_to_mediate_ancient_authors_csv(INPUT_MEDIATE_ANCIENT_AUTHORS_CSV, OUTPUT_CSV_DIR, LIST_AUTHORS_AS_DICTS, sort=True)