import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib.error
import time

df_authors = pd.read_csv(r'\path\MECANO_authors.csv', header = None)

# Create the dictionary using a lambda function
authors_dict = list(map(lambda row: {
    'author': str(row[1]).strip('"'),
    'tags': str(row[2])[str(row[2]).index('('):str(row[2]).index(')') + 1],
    'perseus': str(row[3]).strip('"'),
    'wikipedia': str(row[4]).strip('"'),
    'trimegistos': str(row[5]).strip('"')
}, df_authors.values))

# Set up the SPARQL endpoint
sparql = SPARQLWrapper("https://query.wikidata.org/sparql",
                       agent="Wikidata-classics/1.0 (ripoll_alberola@informatik.uni-leipzig.de)")

# Function to get Wikidata ID for a given Trismegistos ID
def get_wikidata_id(trismegistos_id):
    query = f"""
    SELECT ?item WHERE {{
      ?item wdt:P11252 "{trismegistos_id}".
      # ?item wdt:P31 wd:Q5.  # Ensure the item is a human/person
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    try:
        results = sparql.query().convert()
        if results["results"]["bindings"]:
            return results["results"]["bindings"][0]["item"]["value"].split('/')[-1]
    except urllib.error.HTTPError as e:
        if e.code == 429:  # Too Many Requests
            retry_after = e.headers.get("Retry-After")
            print(f"Rate limit exceeded. Retry after {retry_after} seconds.")
            time.sleep(int(retry_after))  # Wait as per Retry-After header
            return get_wikidata_id(trismegistos_id)  # Retry the request
        else:
            print(f"Error retrieving data: {e}")

# Populate the dictionary with Wikidata IDs
for author in authors_dict:
    trismegistos_id = author.get('trismegistos_id')
    if trismegistos_id:
        author['wikidata_id'] = get_wikidata_id(trismegistos_id)
        time.sleep(1)  # Throttle requests to avoid rate limits

# authors_dict['Homeros']['wikidata_id'] = 'Q6691'
# authors_dict['Scriptores Historiae Augustae']['wikidata_id'] = 'Q9334638'
# authors_dict['Appendix Vergiliana']['wikidata_id'] = 'Q102338318'
# authors_dict['Poseidippos of Pella']['wikidata_id'] = 'Q1392801'
# authors_dict['Marcus Manilius']['wikidata_id'] = 'Q352684'

for author_entry in authors_dict:
    if author_entry['author'] == 'Poseidippos of Pella':
        author_entry['wikidata_id'] = 'Q1392801'
    elif author_entry['author'] == 'Marcus Manilius':
        author_entry['wikidata_id'] = 'Q352684'

# see in author dictionary how many wikidata IDs are empty
nan_entries = [author for author in authors_dict if author.get('wikidata_id') == None]
nan_entries

# Function to get aliases for a given Wikidata ID
def get_aliases(wikidata_id):
    query = f"""
    SELECT ?alias WHERE {{
      wd:{wikidata_id} skos:altLabel ?alias.
      FILTER(LANG(?alias) IN ("en","la","fr","de","es","it"))  # Filter languages
    }}
    ORDER BY ?alias
    """
    
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    
    # Extract aliases from the results
    aliases = [result["alias"]["value"] for result in results["results"]["bindings"]]
    return aliases

# Populate the dictionary with aliases for each author
for author in authors_dict:
    wikidata_id = author.get('wikidata_id')
    if wikidata_id:
        author['aliases'] = get_aliases(wikidata_id)

# Take item titles (in selected languages) and add them as aliases
def get_item_name(wikidata_id):
    query = f"""
    SELECT ?itemName WHERE {{
      wd:{wikidata_id} rdfs:label ?itemName.
      FILTER(LANG(?itemName) IN ("en","la","fr","de","es","it"))  # Filter languages
    }}
    """
    
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    
    # Extract item name from the results
    # item_name = results["results"]["bindings"][0]["itemName"]["value"] if results["results"]["bindings"] else None
    item_name = [result["itemName"]["value"] for result in results["results"]["bindings"]]
    # print(item_name)
    return item_name

# Populate the dictionary with item name and aliases for each author
for author in authors_dict:
    wikidata_id = author.get('wikidata_id')
    if wikidata_id:
        item_name = get_item_name(wikidata_id)
        # Append the item_name to the aliases list if item_name is not None
        if item_name:
            author['aliases'].extend(item_name)

# handle anonymous authors
for author_entry in authors_dict:
    if author_entry['author'] == 'Anonymi of the Hymni Homerici':
        author_entry['aliases'] = ['Hymni Homerici']
    elif author_entry['author'] == 'Anonymi of the Orphica':
        author_entry['aliases'] = ['Orphica']
    elif author_entry['author'] == 'Theophilos of Kaisareia':
        author_entry['aliases'] = ['Theophilos of Kaisareia']
    elif author_entry['author'] == 'Anonymi of the Corpus Hermeticum':
        author_entry['aliases'] = ['Corpus Hermeticum']
    elif author_entry['author'] == 'Anonymi of the Panegyrici Latini':
        author_entry['aliases'] = ['Panegyrici Latini']

# Remove aliases that are repeated in more than one author
for author in authors_dict:
    # Convert all aliases to lowercase and remove duplicates
    author['aliases'] = list(set(alias.lower() for alias in author['aliases']))

# Eliminate alias 'césar' from Augustus mentions (it is already included as an alias for Julius Caesar)
for author in authors_dict:
    if author.get('author') == 'Augustus':
        author['aliases'].remove('césar')
        author['aliases'].remove('gaius julius caesar'
                                 
# saint clement of rome vs saint clement of alexandria
for author in authors_dict:
    if author.get('author') == 'Clemens of Alexandria':
        author['aliases'].remove('saint clement')

# seneca rhetor vs seneca the younger
for author in authors_dict:
    if author.get('author') == 'Seneca rhetor':
        author['aliases'].remove('lucius annaeus seneca')

# ephraem syrus vs ephraem graecus
for author in authors_dict:
    if author.get('author') == 'Ephraem Graecus':
        author['aliases'] = ['ephraem graecus']
    if author.get('author') == 'Ephraem of Syria':
        author['aliases'].remove('ephraem graecus')

# giustino alias for Iustinus and iustinus martyr
for author in authors_dict:
    if author.get('author') == 'Iustinus martyr':
        author['aliases'].remove('giustino')
        author['aliases'].remove('justin')