# Canons Across Time: Compiling Lists of Ancient Authors with Wikidata

Luisa Ripoll-Alberola<sup>1,*</sup>, Marin-Marie Le Bris<sup>2</sup>, Jonas Paul Fischer<sup>3</sup>

<sup>1</sup> Computational Humanities Group, Leipzig University. ORCID: 0009-0001-4611-448X

<sup>2</sup> Cultural and Literary Studies, Radboud University. ORCID: 0009-0009-8459-5358

<sup>3</sup> Computational History Group, University of Helsinki. ORCID: 0009-0008-6283-3637

<sup>*</sup> Corresponding author: ripoll_alberola@informatik.uni-leipzig.de

*Abstract: Canons are lists. When studying the processes of canon formation, one is therefore inevitably faced with the difficulties of compiling lists. In this paper, we present three case studies in which Wikidata was used to elaborate lists of ancient Greek and Latin authors to trace their presence in different corpora: contemporary academic articles, 20th-century French press, and Early Modern print. Detailing workflows to retrieve, enrich, or reconcile the data available on various databases<sup>+</sup>, this contribution illustrates the possibilities and challenges presented by Wikidata when building transferable methodologies for canonisation studies.*

<sup>+</sup>See scripts and data on LINK.

### More details about what can be found in this repository:

**Use Case 1** deals with the presence of ancient authors in the academic discourse of the late 20th century. In the correspondent folder, the following data is present: 

- MECANO.csv is the first list of 200 authors provided directly by the Trismegistos database (acknowledgements to prof. Mark Depauw).
- aliases.csv is the enriched version achieved with Wikidata, in which several aliases of the authors, in several languages, were saved.
- In the code query-1.py one can find the Wikidata queries through which aliases.csv was obtained. 

**Use Case 2** aims at devising a list of ancient authors to look for in a corpus of French newspapers from the Third Republic (*Troisième République*, 1870-1940).

This undertaking calls for a reliable and transparent way of gauging and tracing the evolution of the Graeco-Roman canon over the period under consideration. While the list of authors presented in **use case 1** (here called 'trismegistos_authors.csv’) constitutes a useful starting point, it does not provide any operable quantitative measurement to assess the relative ranking of its entries. 

We therefore turn to the MEDIATE database, which aggregates information “on books and collectors extracted from a Sandbox corpus of 600 smaller [European] private library (sales) catalogues”, dating from 1665 to 1830. We extract all ‘ancient authors’ (i.e. born between 900 BCE and 500 CE), and the respective number of works (items) found in those catalogues, together with the number of collections in which those works appear (out of 600).

Nevertheless, the missing (metada)data (notably DOB) and unstable identifiers (VIAF cluster IDs) used by MEDIATE make it difficult to assess whether we have been able to identify *all* relevant Graeco-Roman authors registered on the database. To make sure we do not miss any major author, we attempt to compare the initial results from MEDIATE with the ‘Trismegistos authors’ list presented in use case 1. 

The issue is that MEDIATE uses VIAF cluster IDs as unique identifiers while Trismegistos authors have their own identifiers.

We therefore use Wikidata and its QIDs as a means to compare the entries from both lists and produce an extended and enriched table of ancient authors found on the MEDIATE database.

**Step 1**: ‘01_cleaning_mediate_results_xlsx.py’ is the script used to clean the initial results obtained from the MEDIATE database when filtering authors with DOB. Its main result is the following dataset:

- ‘ancient_authors_-900_500_mediate_cleaned_results.csv’ (see input > initial_author_lists > mediate > csv)

**Step 2**: ‘02_retrieving_wikidata_info_mediate_cleaned_results.py’ is the script used to retrieve the QIDs associated with the ‘cleaned’ entries obtained from MEDIATE in **Step 1** (which will enable the comparison with the Trismegistos list of authors in step 4). It also retrieves English, French, Latin labels and aliases, as well as writing languages for all the queried authors. Its main result is the following dataset (an ‘enriched’ version of ‘ancient_authors_-900_500_mediate_cleaned_results.csv’):

- ‘02_20250912_mediate_ancient_authors_wiki_labelled_last.csv’ 

**Step 3**: ‘03_retrieving_wikidata_info_trismegistos_authors.py’ is the script used to retrieve the QIDs associated with the entries from the ‘Trismegistos authors’ list (akin to what is done in **Use Case 1**). As in **Step 2**, it also retrieves English, French, Latin labels and aliases, as well as writing languages for all queried authors. Its main output is the following dataset (an ‘enriched’ version of ‘trismegistos_authors.csv’):

- ‘03_20250914_trismegistos_ancient_authors_wiki_labelled’ 

**Step 4**: ‘04_comparing_mediate_trismegistos_qids_authors.py’ is the script used to compare the ancient authors found on the MEDIATE database with the list of Trismegistos authors, based on their retrieved QIDs (in **Step 2** and **Step 3**). Its main output is the set of ‘exclusive’ Trismegistos authors – i.e. Trismegistos authors that have no direct match in the so-far-obtained list of MEDIATE ancient authors:

- ‘04_20250914_exclusive_trismegistos_authors_qids’

**Step 5**: '05_matching_exclusive_trismegistos_authors_to_existing_mediate_authors.py’ is the script used to (a) save the entire list of ‘existing’ MEDIATE authors in JSON format and extract numeric VIAF cluster IDs; (b) retrieve VIAF cluster IDs associated with ‘exclusive’ Trismegistos authors, by querying Wikidata; (c) match the set of ‘exclusive’ Trismegistos authors against all MEDIATE authors based on VIAF cluster IDs; and (d) concatenate and augment the so-far-obtained list of MEDIATE ancient authors with the matched ‘exclusive’ Trismegistos authors based on VIAF IDs. Its main results are the following datasets:

- ‘05_20250914_unmatched_exclusive_trismegistos_authors.csv’
- ‘05_20250914_concatenated_mediate_ancient_authors.csv’ 

**Step 6** ‘06_manually_adding_authors_to_final_mediate_csv.py’ is the script used to add authors found in the ‘05_20250914_unmatched_exclusive_trismegistos_authors.csv’ to the previously arrived at‘05_20250914_concatenated_mediate_ancient_authors.csv’, by manually searching for these remaining ‘exclusive’ Trismegistos authors within the JSON table containing all existing authors from the MEDIATE database. Its main output is the following dataset:

- ‘06_20250914_updated_mediate_ancient_authors.csv’

**Use Case 3** focuses on the reception of ancient authors in Early Modern print in Great Britain and France. The data collected here extends beyond the immediate focus of the use case though and lays the groundwork for future work with the data Wikidata can provide on ancient authors. In the corresponding folder, the following data is present both as CSV files and underlying Python code:

- ancient_authors_wikidata_with_precision is the basis of all other queries in this folder. It provides a list of all authors in Wikidata that have an ancient world related identifier assigned to them. Besides their Q-ID and label in English it also lists VIAF identifiers, Bibliothèque Nationale de France identifiers, as well as birth, death and floruit years together with a precision indicator for these dates.
- ancient_authors_wikidata_author_languages collects the languages the ancient authors wrote in, their native language, spoken language and the language of the names of their works.
- ancient_authors_wikidata_ids collects all the ancient world related identifiers for ancient authors in Wikidata.
- ancient_authors_wikidata_item_metrics collects some metrics for the ancient authors in Wikidata like the amount of statements associated with them, the number of identifiers, the number of sitelinks, the number of languages the author has a Wikipedia page in and the language codes for these languages.
- ancient_authors_wikidata_labels_aliases collects the labels and aliases for the ancient authors in all the languages they are available in.

