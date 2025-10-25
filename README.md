# Canons Across Time: Compiling Lists of Ancient Authors with Wikidata

Luisa Ripoll-Alberola<sup>1,*</sup>, Marin-Marie Le Bris<sup>2</sup>, Jonas Paul Fischer<sup>3</sup>

<sup>1</sup> Computational Humanities Group, Leipzig University. ORCID: 0009-0001-4611-448X

<sup>2</sup> Radboud Institute for Culture & History (RICH), Radboud University. ORCID: 0009-0009-8459-5358

<sup>3</sup> Computational History Group, University of Helsinki. ORCID: 0009-0008-6283-3637

<sup>*</sup> Corresponding author: ripoll_alberola@informatik.uni-leipzig.de

*Abstract: Canons are* lists. *When studying the processes of canon formation, one is therefore inevitably faced with the difficulties of* compiling *lists. In this paper, we present three case studies in which Wikidata was used to elaborate lists of ancient Greek and Latin authors to trace their presence in different corpora: contemporary academic articles, 20th-century French press, and Early Modern print. Detailing workflows to retrieve, enrich, or reconcile the data available on various databases<sup>+</sup>, this contribution illustrates the possibilities and challenges presented by Wikidata when building transferable methodologies for canonisation studies.*

<sup>+</sup>See scripts and data on https://github.com/mecano-dn/canonical-lists.

### More details about what can be found in this repository:

### Case Study 1

**Case Study 1** engages with the presence of ancient authors in the academic discourse of the late 20th century. In the corresponding folder, the following data is present: 

- MECANO_authors.csv is the first list of 207 authors provided directly by the [Trismegistos database](https://www.trismegistos.org/) (acknowledgements to prof. Mark Depauw).
- aliases.csv is the enriched version achieved with Wikidata, in which several aliases of the authors, in multiple languages, were saved.
- In the code query-1.py one can find the Wikidata queries through which aliases.csv was obtained. 

### Case Study 2

**Case Study 2** aims at devising a list of canonical Graeco-Roman authors to look for in a corpus of French newspapers from the Third Republic (*Troisième République*, 1870-1940).

Out of concern for the transparency and flexibility of the adopted criteria of canonicity, it builds on the information aggregated by the [MEDIATE database](https://mediate18.nl/?page=database). Given that the database is still “under construction” - with a few entries lacking important fields -, the dictionary of 207 authors provided in **Case Study 1**, is used as a verification benchmark, to ensure that all *major* ancient authors recorded on MEDIATE are ultimately retrieved. Since MEDIATE employs VIAF cluster IDs (VIAF ID) and Trismegistos (the source of the authorial dictionary from **Case Study 1**) has its own identifiers (TM ID), Wikidata and its Q-IDs therefore serve as a data reconciliation standard, throughout the experiment.

**Step 0**: MEDIATE’s 12,870 entries are sorted based on the recorded authors' date of birth. Classical authors are extracted by accessing the “Persons > Rank by item count” tab, and applying the following filters: (1) “Item roles”: “author”, “author (possible)”, “author (attributed)”; (2) “Date of birth”: “-900”; “500”. The results are exported in XLSX format by adding “&_export=xlsx” at the end of the query’s URL. In addition, the table of *all* (12,870) recorded authors (including “possible” and “attributed”) is downloaded in JSON format by adding “&_export=json” at the end of the query’s URL. See:

<ul><li> “ancient_authors_-900_500_mediate_raw_results.xlsx” (input > initial_author_lists > mediate > csv > raw results)
</li>
<li> “mediate_all_authors_table_raw.json” (input > initial_author_lists > mediate > json)
</li></ul>

**Step 1**: the script “01_cleaning_mediate_results_xlsx.py” is used to clean the initial (raw) table of classical authors obtained from MEDIATE. Its main result is:

<ul><li> “ancient_authors_-900_500_mediate_cleaned_results.csv” (input > initial_author_lists > mediate > csv)
</li></ul>

**Step 2**: “02_retrieving_wikidata_info_mediate_cleaned_results.py” retrieves the Q-IDs associated with the cleaned MEDIATE entries from **Step 1** (which enables the comparison with the Trismegistos dictionary in **Step 4**). It also collects English, French, Latin labels and aliases, as well as writing languages for all queried authors. Its main output is an *enriched* version of “ancient_authors_-900_500_mediate_cleaned_results.csv”:

<ul><li> “02_20250912_mediate_ancient_authors_wiki_labelled_last.csv” (output > authors_csv > 02_mediate_ancient_authors_csv_wiki > 02_last)
</li></ul>

**Step 3**: the script “03_retrieving_wikidata_info_trismegistos_authors.py” retrieves the Q-IDs associated with the entries from the Trismegistos dictionary (akin to what is done in **Case Study 1**). As in **Step 2**, it also collects English, French, Latin labels and aliases, as well as writing languages for all queried authors. Its main result is an enriched version of “trismegistos_authors.csv”:

<ul><li> “03_20250914_trismegistos_ancient_authors_wiki_labelled.csv’ 
</li></ul>

**Step 4**: “04_comparing_mediate_trismegistos_qids_authors.py” compares the ancient authors found on MEDIATE with the list of Trismegistos authors, based on their retrieved QIDs (in **Step 2** and **Step 3**). Its main output is the set of *exclusive* Trismegistos authors - i.e. Trismegistos authors who have no direct match in the MEDIATE list obtained so far:

<ul><li> "04_20250914_exclusive_trismegistos_authors_qids.csv”
</li></ul>

**Step 5**: “05_matching_exclusive_trismegistos_authors_to_existing_mediate_authors.py” is the script used to (a) extract numeric VIAF cluster IDs for all authors contained in the (raw) JSON table downloaded from MEDIATE in **Step 0**, and add them as a new field for each entry; (b) retrieve VIAF IDs associated with exclusive Trismegistos authors, by querying Wikidata; (c) match the set of exclusive Trismegistos authors against *all* MEDIATE authors based on VIAF IDs; and (d) concatenate the cleaned list of MEDIATE classical authors with the *matched* exclusive Trismegistos authors based on VIAF IDs. Its main results are the following datasets:

<ul>
<li> “05_20250914_unmatched_exclusive_trismegistos_authors.csv”
</li>
<li> “05_20250914_concatenated_mediate_ancient_authors.csv”
</li>
</ul>

**Step 6** “06_manually_adding_authors_to_final_mediate_csv.py” is the script used to add authors found in “05_20250914_unmatched_exclusive_trismegistos_authors.csv” to the previously arrived at “05_20250914_concatenated_mediate_ancient_authors.csv”, by manually searching for these remaining exclusive Trismegistos authors within the JSON table containing all existing authors from the MEDIATE database. Its main output is:

<ul>
<li> “06_20250914_updated_mediate_ancient_authors.csv”
</li>
</ul>

### Case Study 3

**Case Study 3** focuses on the reception of ancient authors in Early Modern print in Great Britain and France as well as the representation of those authors in Wikidata. However, the data collected here extends beyond the immediate focus of the case study and lays the groundwork for future undertakings with the information Wikidata can provide on ancient authors. In the corresponding folder, the following data is present both as CSV files and underlying Python code:

- ancient_authors_wikidata_with_precision is the basis of all other queries in this folder. It provides a list of all authors in Wikidata that have an ancient world related identifier assigned to them. Besides their Q-ID and label in English it also lists VIAF identifiers, Bibliothèque Nationale de France identifiers, as well as birth, death and floruit years together with a precision indicator for these dates.
- ancient_authors_wikidata_author_languages collects the languages the ancient authors wrote in, their native language, spoken language and the language of the names of their works.
- ancient_authors_wikidata_ids collects all the ancient world related identifiers for ancient authors in Wikidata.
- ancient_authors_wikidata_item_metrics collects some metrics for the ancient authors in Wikidata like the amount of statements associated with them, the number of identifiers, the number of sitelinks, the number of languages the author has a Wikipedia page in and the language codes for these languages.
- ancient_authors_wikidata_labels_aliases collects the labels and aliases for the ancient authors in all the languages they are available in.
- top_30_authors_table.R is the code that creates the table displaying the top 30 ancient authors in Wikidata as seen in the article accompanying this data release.

