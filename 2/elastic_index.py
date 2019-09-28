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
import argparse

DOC_RE = re.compile("(\d+).txt")
META_RE = re.compile("(\d+).json")
INDEX_BODY = {
    "mappings": {
        "properties": {
            "title": { "type": "text" },
            "body": { "type": "text" }
        }
    }
}
ELASTIC_HOST, ELASTIC_USER, ELASTIC_PWD = open("credentials.txt", "r").read().split()

class Document:
    def indexAction(self):
        return {
            "_op_type": "index",
            "_index": self.index,
            "_id": self.id,
            "url": self.url,
            "title": self.title,
            "body": self.content
        }


class ParsedDocument(Document):
    def __init__(self, fn, doc, meta):
        with open(doc, "r") as docf:
            self.content = docf.read()
        with open(meta, "r") as metaf:
            metaj = json.load(metaf)
        self.title = metaj["title"]
        self.url = metaj["url"]
        self.id = int(fn)
        self.index = INDEX_NAME


def genSelfExtractedActions():
    listdir = os.listdir(DOC_DIR)
    print(len(listdir))
    ok = 0
    for fn in listdir:
        match = DOC_RE.match(fn)
        if match:
            doc_fn = os.path.join(DOC_DIR, fn)
            meta_fn = os.path.join(DOC_DIR, "{}.json".format(match.group(1)))
            ok += 1
            yield ParsedDocument(match.group(1), doc_fn, meta_fn).indexAction()
        elif not META_RE.match(fn):
            print(fn)
    print(ok)



class MeasuredAction:
    def __init__(self, pfx):
        self.pfx = pfx

    def __call__(self, action):
        def wrapped(*args, **kwargs):
            start = timeit.default_timer()
            ret = action(*args, **kwargs)
            end = timeit.default_timer()
            print("{} took {} s".format(self.pfx, end - start))
            return ret
        return wrapped


@MeasuredAction("Indexing of extracted documents")
def indexExtracted(es):
    print(esh.bulk(es, genSelfExtractedActions()))


def indexSize(es, index):
    return es.indices.stats(index=index)["_all"]["primaries"]["store"]["size_in_bytes"]


def main():
    global INDEX_NAME, DOC_DIR

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--doc-dir", type=str, default="extracted")
    parser.add_argument("-i", "--index-name", type=str, default="extracted")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    DOC_DIR = args.doc_dir
    INDEX_NAME = args.index_name

    if args.local:
        es = elasticsearch.Elasticsearch()
    else:
        es = elasticsearch.Elasticsearch(hosts=[ELASTIC_HOST],
                                         http_auth=(ELASTIC_USER, ELASTIC_PWD))

    es.indices.create(index=INDEX_NAME, body=INDEX_BODY, ignore=400)

    indexExtracted(es)
    print("Size of index of extracted documents: {} bytes".format(indexSize(es, INDEX_NAME)))



if __name__ == "__main__":
    main()
