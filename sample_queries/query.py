import urllib.parse
import urllib.request
import json

url = 'https://dss.staging.data.humancellatlas.org/v1/search?replica=aws'
values = {
 "es_query": {
   "query": {
     "bool": {
       "must": [
         {
           "match": {
             "manifest.files.name": "analysis.json"
           }
         },
         {
           "match": {
             "files.sample_json.donor.species.ontology": "9606"
           }
         },
         {
           "wildcard": {
             "manifest.files.name": "*fastq.gz"
           }
         }
       ]
     }
   }
 }
}

headers = {"User-Agent": "Mozilla", 'accept': 'application/json', 'content-type': 'application/json'}
data = json.dumps(values)
data = data.encode('ascii') # data should be bytes
req = urllib.request.Request(url, data, headers)
with urllib.request.urlopen(req) as response:
   the_page = response.read()
   print(the_page)
