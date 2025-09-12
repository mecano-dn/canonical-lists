# Canons Across Time: Compiling Lists of Ancient Authors with Wikidata

Luisa Ripoll-Alberola<sup>1,*</sup>, Name 2<sup>2</sup>, Jonas Paul Fischer<sup>3</sup>

<sup>1</sup> Computational Humanities group, Leipzig University. ORCID: 0009-0001-4611-448X

<sup>2</sup> Affiliation 2.

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

**Use Case 3** focuses on the reception of ancient authors in Early Modern print...
