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

QUERIES_XML = "web2008_adhoc.xml" #"output_stemmas.xml" #"web2008_adhoc.xml"
RELEVANT_TABLE = "or_relevant-minus_table.xml"
ELASTIC_HOST, ELASTIC_USER, ELASTIC_PWD = open("credentials.txt", "r").read().split()

def es_search_basic(es, query, index):
    return es.search(size=20, index=index, body={"_source":["_score", "_id"], "query": {"match": {"body": query["qtext"]}}})


def es_search_rprecision(es, query, index):
    return es.search(size=query["size"], index=index, body={"_source":["_score", "_id"], "query": {"match": {"body": query["qtext"]}}})
    
    
def get_query_info(query):
    qid = query.getAttribute("id")
    fc = query.getElementsByTagName("querytext")[0].firstChild
    if fc is None:
        return None, None
    qtext = fc.data
    return qid, qtext


def run_search(es, queries, search, outpath, index):
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
    with open(outpath, "w") as file:
         json.dump(results, file)
    with open(outpath + "_times", "w") as file:
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
        if qid is None:
            continue
            
        if qid in relevant_sizes:
            queries.append({ "qid": qid, "qtext": qtext, "size": relevant_sizes[qid] })
    return queries


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--index", type=str, help="Index to search in", default="extracted")
    parser.add_argument("-o", "--outdir", type=str, help="Output directory name", default="results")
    parser.add_argument("-m", "--mode", type=str, help="Query mode", default="full")
    parser.add_argument("--local", action="store_true", help="Use local elasticsearch")

    args = parser.parse_args()

    if args.local:
        es = elasticsearch.Elasticsearch()
    else:
        es = elasticsearch.Elasticsearch(hosts=[ELASTIC_HOST], http_auth=(ELASTIC_USER, ELASTIC_PWD))
    print("Elasticsearch loaded")
    
    queries = get_queries()
    print("{} queries loaded".format(len(queries)))
    
    if args.mode == "top20" or args.mode == "full":
        run_search(es, queries, search=es_search_basic, outpath=os.path.join(args.outdir, "top20"), index=args.index)
    if args.mode == "rprecision" or args.mode == "full":
        run_search(es, queries, search=es_search_basic, outpath=os.path.join(args.outdir, "rprecision"), index=args.index)
    
    
if __name__ == "__main__":
    main()
