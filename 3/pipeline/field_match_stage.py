from pipeline.pipeline import PipelineStage, PipedInput
import json
import sys, os
from utils import *


class FieldMatchStage(PipelineStage):
    def __init__(self, queries):
        self.queries = queries

    def accept(self, consumer_input: PipedInput):
        doc_id = consumer_input.get_doc_id()
        title = consumer_input.get_meta()["title"]
        body = consumer_input.get_text()

        for query in self.queries:
            pair_key = "{}:{}".format(query.qid, doc_id)
            features = self.features[pair_key]

            features["match_body"] = fraction_of_words(query.text, body)
            features["match_title"] = fraction_of_words(query.text, title)
            features["window"] = shortest_window(query.text, body)

    def dump(self):
        pass
