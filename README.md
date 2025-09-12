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

In **Use Case 2** there are several dataframes dealing with French press...

**Use Case 3** focuses on the reception of ancient authors in Early Modern print in Great Britain and France. The data collected here extends beyond the immediate focus of the use case though and lays the groundwork for future work with the data Wikidata can provide on ancient authors. In the corresponding folder, the following data is present both as CSV files and underlying Python code:

- ancient_authors_wikidata_with_precision is the basis of all other queries in this folder. It provides a list of all authors in Wikidata that have an ancient world related identifier assigned to them. Besides their Q-ID and label in English it also lists VIAF identifiers, Bibliothèque Nationale de France identifiers, as well as birth, death and floruit years together with a precision indicator for these dates.
- ancient_authors_wikidata_author_languages collects the languages the ancient authors wrote in, their native language, spoken language and the language of the names of their works.
- ancient_authors_wikidata_ids collects all the ancient world related identifiers for ancient authors in Wikidata.
- ancient_authors_wikidata_item_metrics collects some metrics for the ancient authors in Wikidata like the amount of statements associated with them, the number of identifiers, the number of sitelinks, the number of languages the author has a Wikipedia page in and the language codes for these languages.
- ancient_authors_wikidata_labels_aliases collects the labels and aliases for the ancient authors in all the languages they are available in.

