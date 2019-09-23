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

DOC_DIR = "extracted"
DOC_RE = re.compile("(\d+).txt")
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
        self.index = "extracted"


def genSelfExtractedActions():
    for fn in os.listdir(DOC_DIR):
        match = DOC_RE.match(fn)
        if match:
            doc_fn = os.path.join(DOC_DIR, fn)
            meta_fn = os.path.join(DOC_DIR, "{}.json".format(match.group(1)))
            yield ParsedDocument(match.group(1), doc_fn, meta_fn).indexAction()



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
    esh.bulk(es, genSelfExtractedActions())


def indexSize(es, index):
    return es.indices.stats(index=index)["_all"]["primaries"]["store"]["size_in_bytes"]


def main():
    es = elasticsearch.Elasticsearch(hosts=[ELASTIC_HOST],
                                     http_auth=(ELASTIC_USER, ELASTIC_PWD))

    es.indices.create(index="extracted", body=INDEX_BODY, ignore=400)

    indexExtracted(es)
    print("Size of index of extracted documents: {} bytes".format(indexSize(es, "extracted")))


if __name__ == "__main__":
    main()
