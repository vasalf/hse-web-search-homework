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

def map_at_k(expected, actual_raw, k):
    if len(actual_raw) == 0:
        return "n/a"
    
    p = 0
    for i in range(1, k):
        p_i, _ = precision_recall(expected, actual_raw[:i])
        p += p_i
    return p / k

def precision_recall(expected, actual_raw):
    actual = set([doc["id"] for doc in actual_raw])
    true_pos = len(actual.intersection(expected))
    
    p = try_divide(true_pos, len(actual))
    r = try_divide(true_pos, len(expected))
    return p, r
    

def main():
    if len(sys.argv) < 3:
        basic_path = "results/basic.json"
        rprecision_path = "results/rprecision.json"
    else:
        basic_path = sys.argv[1]
        rprecision_path = sys.argv[2]
    
    expected_raw = minidom.parse(RELEVANT_TABLE).getElementsByTagName("task")
    actual_raw = load_json(basic_path)
    actual_rprecision_raw = load_json(rprecision_path)
    
    metrics = {}
    for task in expected_raw:
        qid = task.getAttribute("id")
        if qid not in actual_raw:
            continue
            
        expected = set([doc.getAttribute("id") 
                        for doc in task.getElementsByTagName("document") 
                        if doc.getAttribute("relevance") == "vital"])
                    
        p20, r20 = precision_recall(expected, actual_raw[qid])    
        map20 = map_at_k(expected, actual_raw[qid], 20)
        rp, _ = precision_recall(expected, actual_rprecision_raw[qid])
        
        metrics[qid] = { "p20": p20, "r20": r20, "map20": map20, "rprecision": rp }
        print("{}: p@20={} r@20={} map@20={}, rprec={}".format(qid, p20, r20, map20, rp))
        
    with open(os.path.join("results", "metrics.json"), 'w') as file:
        json.dump(metrics, file)
        
if __name__ == "__main__":
    main()
