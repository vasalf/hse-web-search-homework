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
            features = get_or_create_features(self.features, pair_key)

            features["match_body"] = count_match(query.text, body)
            features["match_title"] = count_match(query.text, title)

    def dump(self):
        pass


def get_or_create_features(features, key):
    if key not in features:
        features[key] = {}
    return features[key]


def count_match(s1, s2):
    return 1.0
