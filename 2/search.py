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

def es_search_basic(es, q):
    return es.search(size=20, index="extracted", body={"_source":["_score", "_id"], "query": {"simple_query_string": {"query": q, "fields": ["body"]}}})

def get_query_info(query):
    qid = query.getAttribute("id")
    qtext = query.getElementsByTagName("querytext")[0].firstChild.data
    return qid, qtext.replace("\n", "")

def run_search(es, queries, search, outname):
    results = {}
    for i, (qid, qtext) in enumerate(queries):
        try:
            print("running query {}/{} ({}): {}".format(i, len(queries), qid, qtext))
            search_result = search(es, qtext)
            results[qid] = [ {"score": hit['_score'], "id": hit["_id"]} for hit in search_result["hits"]["hits"] ]
        except Exception as e:
            print("query {} failed: {}".format(qid, qtext), file=sys.stderr)
            print(e, file=sys.stderr)
    with open(os.path.join("results", "{}.json".format(outname)), 'w') as file:
        json.dump(results, file)

def get_queries():
    task_ids = set([task.getAttribute("id") for task in minidom.parse(RELEVANT_TABLE).getElementsByTagName("task")])
    all_queries = minidom.parse(QUERIES_XML).getElementsByTagName("task")
    queries = []
    for i, query in enumerate(all_queries):
        qid, qtext = get_query_info(query)
        if qid in task_ids:
            print("{} queries loaded".format(i + 1), end="\r")
            queries.append((qid, qtext))
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
    
if __name__ == "__main__":
    main()
