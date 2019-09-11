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

# subgraph without isolated nodes
def subgraph(g, nodes):
    result = nx.DiGraph()
    for f, t in g.subgraph(nodes).edges():
        result.add_edge(f, t)
    return result


class GraphBuilder(PipelineStage):
    def __init__(self, pagerank_top):
        self.graph = nx.DiGraph()
        self.pagerank_top = pagerank_top

        # URL-to-index mappings
        self.indexes = {}
        self.inv_indexes = {}
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

        for l in meta["leadsTo"]:
            full = absoluteURL(base, l)
            i = self.index_of(full)
            if i != base_i:
                self.graph.add_edge(base_i, i)

    def dump_graph(self, name, g):
        with open(os.path.join("results", "{}.gml".format(name)), 'w') as file:
            file.write("graph\n[\n")
            for k in g.nodes():
                file.write("  node\n  [\n   id {}\n   label \"{}\"\n  ]\n".format(k, self.inv_indexes[k]))
            for to_v, from_v in g.edges():
                file.write("  edge\n  [\n   source {}\n   target {}\n  ]\n".format(from_v, to_v))
            file.write("]\n")

    def dump(self):
        # we filter only URLs from documents, removing all other links from leadsTo
        result = subgraph(self.graph, self.bases)
        self.dump_graph("graph_full", result)
        print("Graph saved to results/graph_full.gml")

        pr = nx.pagerank(result)
        top_pr = sorted(pr, key=pr.get, reverse=True)[:(self.pagerank_top)]
        top_graph = subgraph(result, top_pr)
        self.dump_graph("graph_top", top_graph)
        print("Graph saved to results/graph_top.gml")
