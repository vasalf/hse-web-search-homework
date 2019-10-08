from pipeline.pipeline import PipelineStage, PipedInput
import json
import sys, os
from utils import *


class FieldMatchStage(PipelineStage):
    def __init__(self, queries, docs_to_queries):
        self.queries = queries
        self.docs_to_queries = docs_to_queries

    def accept(self, consumer_input: PipedInput):
        doc_id = consumer_input.get_doc_id()
        title = consumer_input.get_meta()["title"]
        body = consumer_input.get_text()

        for qid in self.docs_to_queries.get(doc_id, []):
            pair_key = "{}:{}".format(qid, doc_id)
            features = self.features[pair_key]

            text = self.queries[qid]
            features["match_body"] = fraction_of_words(text, body)
            features["match_title"] = fraction_of_words(text, title)
            features["window"] = shortest_window(text, body)

    def dump(self):
        pass
