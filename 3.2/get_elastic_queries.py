#!/usr/bin/python3

import xml.dom.minidom
import elasticsearch


def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


class Document:
    def __init__(self, doc_id, relevance):
        self.id = doc_id
        self.relevance = relevance
        self.url = None


class Query:
    def __init__(self, arw):
        self.arw = arw
        self.docs = []
        self.text = None

    def add_document(self, doc):
        self.docs.append(doc)

    def set_text(self, text):
        self.text = text

    def prepare(self):
        self.ids = dict()
        for d in self.docs:
            self.ids[d.id] = d


def es_search_basic(es, query, index):
    return es.search(size=100, index=index, body={"_source":["_score", "_id", "url"], "query": {"match": {"body": query}}})


relevance_dom = xml.dom.minidom.parse("relevant_table_2009.xml")
queries = dict()
for tag in relevance_dom.getElementsByTagName("task"):
    arw = tag.getAttribute("id")
    q = Query(arw)
    for doc_tag in tag.getElementsByTagName("document"):
        d = Document(doc_tag.getAttribute("id"),
                     doc_tag.getAttribute("relevance"))
        q.add_document(d)
    queries[q.arw] = q

adhoc_dom = xml.dom.minidom.parse("web2008_adhoc.xml")
for tag in adhoc_dom.getElementsByTagName("task"):
    arw = tag.getAttribute("id")
    querytext = getText(tag.getElementsByTagName("querytext")[0].childNodes)
    if arw in queries:
        queries[arw].set_text(querytext)

es = elasticsearch.Elasticsearch()
print('<?xml version="1.0" encoding="windows-1251"?>')
print('<taskDocumentMatrix xmlns="http://www.romip.ru/common/merged-results" type="or (relevant-minus)">')
for q in queries.values():
    q.prepare()
    es_ans = es_search_basic(es, q.text, "lemmatized")
    for hit in es_ans["hits"]["hits"]:
        if hit["_id"] in q.ids:
            q.ids[hit["_id"]].url = hit["_source"]["url"]
    print('<task id="{}">'.format(q.arw))
    for d in q.docs:
        if d.url is not None:
            print('  <document id="{}" relevance="{}"/>'.format(d.url, d.relevance))
    print('</task>')
print("</taskDocumentMatrix>")
