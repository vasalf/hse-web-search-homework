from pipeline.pipeline import PipelineStage, PipedInput
from utils import *
from collections import defaultdict
import json
from urllib.parse import urlparse, urlunparse, urljoin
import sys, os
import networkx as nx

def absoluteURL(base, relative):
    try:
        pr = urlparse(relative)
        if pr.scheme == "javascript" or pr.path == "None" or pr.scheme == "mailto":
            return None

        if pr.scheme != "":
            return relative
        return urljoin(base, relative)
    except:
        print("Error parsing {}".format(relative))
        return None

def subgraph(g, nodes):
    result = nx.DiGraph()
    for n in nodes:
        result.add_node(n)
    for f, t in g.subgraph(nodes).edges():
        result.add_edge(f, t)
    return result


class PageRankStage(PipelineStage):
    def __init__(self):
        self.graph = nx.DiGraph()

        # URL-to-index mappings
        self.indexes = {}
        self.inv_indexes = {}
        self.doc_ids = {}
        self.last_index = 0

        # URLs from pipeline
        self.bases = set()

    def index_of(self, s):
        if s in self.indexes:
            return self.indexes[s]
        self.indexes[s] = self.last_index
        self.inv_indexes[self.last_index] = s
        self.last_index += 1
        return self.last_index - 1

    def accept(self, consumer_input: PipedInput):
        meta = consumer_input.get_meta() 
        base = meta["url"]

        base_i = self.index_of(base)
        self.bases.add(base_i)

        doc_id = consumer_input.get_doc_id()
        self.doc_ids[base_i] = doc_id

        for l in meta["leadsTo"]:
            full = absoluteURL(base, l)
            i = self.index_of(full)
            if i != base_i:
                self.graph.add_edge(base_i, i)

    def dump(self):
        result = subgraph(self.graph, self.bases)

        pr = nx.pagerank(result)

        for k, v in pr.items():
            self.doc_features[self.doc_ids[k]]["pagerank"] = v
