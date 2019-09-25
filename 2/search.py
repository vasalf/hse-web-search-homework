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
import argparse

QUERIES_XML = "web2008_adhoc.xml"
RELEVANT_TABLE = "or_relevant-minus_table.xml"
ELASTIC_HOST, ELASTIC_USER, ELASTIC_PWD = open("credentials.txt", "r").read().split()

def es_search_basic(es, query, index):
    return es.search(size=20, index=index, body={"_source":["_score", "_id"], "query": {"match": {"body": query["qtext"]}}})


def es_search_rprecision(es, query, index):
    return es.search(size=query["size"], index=index, body={"_source":["_score", "_id"], "query": {"match": {"body": query["qtext"]}}})
    
    
def get_query_info(query):
    qid = query.getAttribute("id")
    qtext = query.getElementsByTagName("querytext")[0].firstChild.data
    return qid, qtext


def run_search(es, queries, search, outname, index):
    results = {}
    qtimes = {}
    for i, query in enumerate(queries):
        try:
            start = timeit.default_timer()
            search_result = search(es, query, index)
            time = timeit.default_timer() - start
            
            qid = query["qid"]
            qtimes[qid] = time
            results[qid] = [ {"score": hit["_score"], "id": hit["_id"]} for hit in search_result["hits"]["hits"] ]
            print("Query {}/{} ({}): {}".format(i, len(queries), qid, query["qtext"]), end="")
            print(" in {0:.3f} s".format(time))
        except Exception as e:
            print("Query failed: {}".format(query), file=sys.stderr)
            print(e, file=sys.stderr)
    with open(outname, "w") as file:
         json.dump(results, file)
    with open(outname + "_times", "w") as file:
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

    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--index", type=str, help="Index to search in", default="extracted")
    parser.add_argument("-o", "--output", type=str, help="Output file name", default="output")
    parser.add_argument("-m", "--mode", type=str, help="Query mode", default="basic")

    args = parser.parse_args()

    es = elasticsearch.Elasticsearch(hosts=[ELASTIC_HOST], http_auth=(ELASTIC_USER, ELASTIC_PWD))
    print("Elasticsearch loaded")
    
    queries = get_queries()
    print("{} queries loaded".format(len(queries)))
    
    if args.mode == "basic":
        search_mode = es_search_basic
    elif args.mode == "rprecision":
        search_mode = es_search_rprecision
    else:
        print("No such mode {}".format(args.mode), file=sys.stderr)
        return
    
    run_search(es, queries, search=search_mode, outname=args.output, index=args.index)
    
    
if __name__ == "__main__":
    main()
