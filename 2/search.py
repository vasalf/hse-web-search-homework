#!/usr/bin/python3

import elasticsearch
import elasticsearch.helpers as esh
import errno
import json
import re
import os
import os.path
import timeit
import xml.dom.minidom as minidom
import sys

QUERIES_XML = "web2008_adhoc.xml"
RELEVANT_TABLE = "or_relevant-minus_table.xml"

def es_search_basic(es, query):
    return es.search(size=20, index="extracted", body={"_source":["_score", "_id"], "query": {"match": {"body": query["qtext"]}}})

def es_search_rprecision(es, query):
    return es.search(size=query["size"], index="extracted", body={"_source":["_score", "_id"], "query": {"match": {"body": query["qtext"]}}})
    
def get_query_info(query):
    qid = query.getAttribute("id")
    qtext = query.getElementsByTagName("querytext")[0].firstChild.data
    return qid, qtext

def run_search(es, queries, search, outname):
    results = {}
    qtimes = {}
    for i, query in enumerate(queries):
        try:
            start = timeit.default_timer()
            search_result = search(es, query)
            time = timeit.default_timer() - start
            
            qid = query["qid"]
            qtimes[qid] = time
            results[qid] = [ {"score": hit['_score'], "id": hit["_id"]} for hit in search_result["hits"]["hits"] ]
            print("{}: query {}/{} ({}): {}".format(outname, i, len(queries), qid, query["qtext"]), end="")
            print(" in {0:.3f} s".format(time))
        except Exception as e:
            print("query failed: {}".format(query), file=sys.stderr)
            print(e, file=sys.stderr)
    with open(os.path.join("results", "{}.json".format(outname)), 'w') as file:
         json.dump(results, file)
    with open(os.path.join("results", "{}.json".format("time_" + outname)), 'w') as file:
         json.dump(qtimes, file)

def get_relevant_sizes(tasks):
    relevant_sizes = {}
    for task in tasks:
        qid = task.getAttribute("id")
        relevant_size = len([doc.getAttribute("id") 
                            for doc in task.getElementsByTagName("document") 
                            if doc.getAttribute("relevance") == "vital"])
        relevant_sizes[qid] = relevant_size
    return relevant_sizes
        

def get_queries():
    tasks = minidom.parse(RELEVANT_TABLE).getElementsByTagName("task")
    relevant_sizes = get_relevant_sizes(tasks)
    
    all_queries = minidom.parse(QUERIES_XML).getElementsByTagName("task")
    queries = []
    for i, query in enumerate(all_queries):
        qid, qtext = get_query_info(query)
        if qid in relevant_sizes:
            queries.append({ "qid": qid, "qtext": qtext, "size": relevant_sizes[qid] })
    return queries

def main():
    host = os.getenv("ELASTIC_HOST")
    passwd = os.getenv("ELASTIC_PASS")
    if host is None or passwd is None:
        print("ELASTIC_HOST or ELASTIC_PASS env vars not set", file=sys.stderr)
        return
    es = elasticsearch.Elasticsearch(hosts=[host], http_auth=("elastic", passwd))
    print("Elasticsearch loaded")
    queries = get_queries()
    print("{} queries loaded".format(len(queries)))
    run_search(es, queries, es_search_basic, "basic")
    run_search(es, queries, es_search_rprecision, "rprecision")
    
if __name__ == "__main__":
    main()
