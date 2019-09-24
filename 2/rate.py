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

RELEVANT_TABLE = "or_relevant-minus_table.xml"

def load_json(path):
    with open(path) as file: 
        json_res = json.load(file)
        return json_res

def try_divide(a, b):
    if b == 0:
        return "n/a"
    return a / b

def main():
    if len(sys.argv) < 2:
        result_path = "results/basic.json"
    else:
        result_path = sys.argv[1]
    
    expected_raw = minidom.parse(RELEVANT_TABLE).getElementsByTagName("task")
    actual_raw = load_json(result_path)
    
    metrics = {}
    for task in expected_raw:
        qid = task.getAttribute("id")
        if qid not in actual_raw:
            continue
            
        expected = set([doc.getAttribute("id") 
                for doc in task.getElementsByTagName("document") 
                if doc.getAttribute("relevance") == "vital"])
        actual = set([doc["id"] for doc in actual_raw[qid]])
        
        true_pos = len(actual.intersection(expected))
        metrics[qid] = { "p20": try_divide(true_pos, len(actual)), "r20": try_divide(true_pos, len(expected)) }
        print("{}: p@20={} r@20={}".format(qid, metrics[qid]['p20'], metrics[qid]['r20']))
        
    with open(os.path.join("results", "{}.json".format("pr20")), 'w') as file:
        json.dump(metrics, file)
        
if __name__ == "__main__":
    main()
