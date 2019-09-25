#!/usr/bin/python3

import argparse
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
        return 0
    return a / b

def map_at_k(expected, actual_raw, k):
    if len(actual_raw) == 0:
        return 0
    
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

    parser = argparse.ArgumentParser()

    parser.add_argument("-t", "--top20", type=str, help="Path to results on top20 search", default="results/top20")
    parser.add_argument("-r", "--rprecision", type=str, help="Path to results on rpecision search", default="results/rprecision")
    parser.add_argument("-o", "--outfile", type=str, help="Output file", default="results/metrics")

    args = parser.parse_args()

    basic_path = args.top20
    rprecision_path = args.rprecision
    
    expected_raw = minidom.parse(RELEVANT_TABLE).getElementsByTagName("task")
    actual_raw = load_json(basic_path)
    actual_rprecision_raw = load_json(rprecision_path)
    
    metrics = {}
    map20 = []
    for task in expected_raw:
        qid = task.getAttribute("id")
        if qid not in actual_raw:
            continue
            
        expected = set([doc.getAttribute("id") 
                        for doc in task.getElementsByTagName("document") 
                        if doc.getAttribute("relevance") == "vital"])
                    
        p20, r20 = precision_recall(expected, actual_raw[qid])    
        map20.append(map_at_k(expected, actual_raw[qid], 20))
        rp, _ = precision_recall(expected, actual_rprecision_raw[qid])
        
        metrics[qid] = { "p20": p20, "r20": r20, "rprecision": rp }
        print("{}: p@20={} r@20={}, rprec={}".format(qid, p20, r20, rp))
    
    result = { "map20": sum(map20) / len(map20), "metrics": metrics }    
    with open(args.outfile, 'w') as file:
        json.dump(result, file)
        
if __name__ == "__main__":
    main()
