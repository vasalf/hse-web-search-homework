import json
import os
import xml.dom.minidom as minidom

from pipeline.text_processor_stage import TextProcessorStage, StopwordsFilter

class Query:
    def __init__(self, qid, text):
        self.qid = qid
        self.text = text

    def __str__(self):
        return "{}: {}".format(self.qid, self.text)

    def get_qid(self):
        return self.qid

    def get_text(self):
        return self.text


def load_relevant_docs(relevant_path):
    tasks = minidom.parse(relevant_path).getElementsByTagName("task")

    relevant_docs = {}
    for task in tasks:
        qid = task.getAttribute("id")
        relevant_docs[qid] = [doc.getAttribute("id")
                              for doc in task.getElementsByTagName("document")
                              if doc.getAttribute("relevance") == "vital"]
    return relevant_docs


def get_query_info(query):
    qid = query.getAttribute("id")
    fc = query.getElementsByTagName("querytext")[0].firstChild
    if fc is None:
        return None, None
    text = fc.data
    return qid, text


def load_queries(queries_path, relevant_docs):
    all_queries = minidom.parse(queries_path).getElementsByTagName("task")
    queries = []
    for query in all_queries:
        qid, text = get_query_info(query)
        text = StopwordsFilter.filter_stopwords(TextProcessorStage.lemmatize(text))
        if qid is not None and text and qid in relevant_docs:
            queries.append(Query(qid, text))

    return queries
