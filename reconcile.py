"""
An OpenRefine reconciliation service for the API provided by
OCLC for FAST.

See API documentation:
http://www.oclc.org/developer/documentation/fast-linked-data-api/request-types

This code is adapted from Michael Stephens:
https://github.com/mikejs/reconcile-demo

Further adapted from CMHarlow and merging in ideas from my SRUSearch webapp
https://github.com/cmharlow/fast-reconcile for CMHarlow
https://kdmortimer.github.io/FASTSRUtest.html for the SRUSearch app

"""

from flask import Flask
from flask import request
from flask import jsonify

import json
from json import dumps
from collections import OrderedDict
from xmljson import BadgerFish
from operator import itemgetter

from xml.etree.ElementTree import fromstring
from sys import version_info

#For scoring results
from fuzzywuzzy import fuzz
import requests

app = Flask(__name__)
bf = BadgerFish(dict_type=OrderedDict) 
#some config
api_base_url = 'http://experimental.worldcat.org/fast/search'
#For constructing links to FAST.
fast_uri_base = 'http://id.worldcat.org/fast/{0}'

#See if Python 3 for unicode/str use decisions
PY3 = version_info > (3,)

#If it's installed, use the requests_cache library to
#cache calls to the FAST API.
try:
    import requests_cache
    requests_cache.install_cache('fast_cache')
except ImportError:
    app.logger.debug("No request cache found.")
    pass


#Map the FAST query indexes to service types
default_query = {
    "id": "/SRUfast/all",
    "name": "All FAST headings",
    "index": "cql.any"
}

refine_to_fast = [
    {
        "id": "/SRUfast/topic",
        "name": "Topical headings",
        "index": "oclc.topic"
    },
    {
        "id": "/SRUfast/geographic",
        "name": "Geographical headings",
        "index": "oclc.geographic"
    },
    {
        "id": "/SRUfast/event",
        "name": "Event headings",
        "index": "oclc.eventName"
    },
    {
        "id": "/SRUfast/title",
        "name": "Uniform title headings",
        "index": "oclc.uniformTitle"
    },
    {
        "id": "/SRUfast/corporate",
        "name": "Corporate name headings",
        "index": "oclc.corporateName"
    },
    {
        "id": "/SRUfast/form",
        "name": "Form headings",
        "index": "oclc.form"
    },
    {
        "id": "/SRUfast/period",
        "name": "Period headings",
        "index": "oclc.period"
    },
    {
        "id": "/SRUfast/LoC",
        "name": "Library of Congress source headings",
        "index": "oclc.altlc"
    }
]
refine_to_fast.append(default_query)


#Make a copy of the FAST mappings.
#Minus the index for
query_types = [{'id': item['id'], 'name': item['name']} for item in refine_to_fast]

# Basic service metadata. There are a number of other documented options
# but this is all we need for a simple service.
metadata = {
    "name": "Fast Reconciliation Service",
    "defaultTypes": query_types,
    "view": {
        "url": "{{id}}"
    }
}


def make_uri(fast_id):
    """
    Prepare a FAST url from the ID returned by the API.
    """
    fid = fast_id.lstrip('fst').lstrip('0')
    fast_uri = fast_uri_base.format(fid)
    return fast_uri


def jsonpify(obj):
    """
    Helper to support JSONP
    """
    try:
        callback = request.args['callback']
        response = app.make_response("%s(%s)" % (callback, json.dumps(obj)))
        response.mimetype = "text/javascript"
        return response
    except KeyError:
        return jsonify(obj)


def search(raw_query, query_type='/SRUfast/all', numRecords = 20, startRecord = 1, sortedResults = False):
    """
    Hit the FAST API for names.
    """
    out = []
    unique_fast_ids = []
    query_type_meta = [i for i in refine_to_fast if i['id'] == query_type]
    if query_type_meta == []:
        query_type_meta = default_query
    query_index = query_type_meta[0]['index']
    try:
        #FAST api requires spaces to be encoded as %20 rather than +
        url = api_base_url + "?query=" + query_index +'+all+"' + raw_query + '"&httpAccept=application/xml&maximumRecords=' 
        url += str(numRecords) + '&startRecord=' + str(startRecord) +'&sortKey=usage&recordSchema=info:srw/schema/1/rdf-v2.0'
        #app.logger.debug("FAST API url is " + url)
        resp = requests.get(url)
        jsonResp = dumps(bf.data(fromstring(resp.content)))
        #print(jsonResp)
        results = json.loads(jsonResp)
        #print(results)
    except Exception as e:
        app.logger.warning(e)
        return out
    for record in (results["{http://www.loc.gov/zing/srw/}searchRetrieveResponse"][ "{http://www.loc.gov/zing/srw/}records"]['{http://www.loc.gov/zing/srw/}record']):
        minRecord = record["{http://www.loc.gov/zing/srw/}recordData"]["{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF"]["{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description"][0]
        #print(minRecord)
        name = minRecord['{http://www.w3.org/2004/02/skos/core#}prefLabel']['$']
        alt = []
        if '{http://www.w3.org/2004/02/skos/core#}altLabel' in minRecord:
            #print(minRecord['{http://www.w3.org/2004/02/skos/core#}altLabel'])
            for altLabel in minRecord['{http://www.w3.org/2004/02/skos/core#}altLabel']:
                alt.append(altLabel)            
        print(alt)
        fid = str(minRecord['{http://purl.org/dc/terms/}identifier']['$'])
        fast_uri = make_uri(fid)
        #The FAST service returns many duplicates.  Avoid returning many of the
        #same result
        #I don't think this does anything in the SRU search
        if fid in unique_fast_ids:
            continue
        else:
            unique_fast_ids.append(fid)
        score_1 = fuzz.token_sort_ratio(raw_query, name)
        scoreBundle = []
        for altLabel in alt:
            scoreBundle.append(fuzz.token_sort_ratio(raw_query, altLabel))
        if scoreBundle:
            score_2 = max(scoreBundle)
        else:
            score_2 = 0
        #Return a maximum score
        score = max(score_1, score_2)

        resource = {
            "id": fast_uri,
            "name": name,
            "score": score,
            "type": query_type_meta[0]
        }
        out.append(resource)
    #Sort this list by score
    sorted_out = sorted(out, key=itemgetter('score'), reverse=True)
    #Refine only will handle top three matches.
    if sortedResults == True:
        return sorted_out
    else:
        return out


@app.route("/reconcile", methods=['POST', 'GET'])
def reconcile():
    #Single queries have been deprecated.  This can be removed.
    #Look first for form-param requests.
    query = request.form.get('query')
    if query is None:
        #Then normal get param.s
        query = request.args.get('query')
        query_type = request.args.get('type', '/SRUfast/all')
    if query:
        # If the 'query' param starts with a "{" then it is a JSON object
        # with the search string as the 'query' member. Otherwise,
        # the 'query' param is the search string itself.
        if query.startswith("{"):
            query = json.loads(query)['query']
        results = search(query, query_type=query_type) #what is this supposed to do?
        return jsonpify({"result": results})
    # If a 'queries' parameter is supplied then it is a dictionary
    # of (key, query) pairs representing a batch of queries. We
    # should return a dictionary of (key, results) pairs.
    queries = request.form.get('queries')
    if queries:
        queries = json.loads(queries)
        results = {}
        for (key, query) in queries.items():
            qtype = query.get('type')
            #If no type is specified this is likely to be the initial query
            #so lets return the service metadata so users can choose what
            #FAST index to use.
            if qtype is None:
                return jsonpify(metadata)
            data = search(query['query'], query_type=qtype)
            results[key] = {"result": data}
        return jsonpify(results)
    # If neither a 'query' nor 'queries' parameter is supplied then
    # we should return the service metadata.
    return jsonpify(metadata)

if __name__ == '__main__':
    from optparse import OptionParser
    oparser = OptionParser()
    oparser.add_option('-d', '--debug', action='store_true', default=False)
    opts, args = oparser.parse_args()
    app.debug = opts.debug
    app.run(host='0.0.0.0')
