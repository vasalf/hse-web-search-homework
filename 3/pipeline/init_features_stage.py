from pipeline.pipeline import PipelineStage, PipedInput


class InitFeaturesStage(PipelineStage):
    def __init__(self, queries, docs_to_queries):
        self.queries = queries
        self.docs_to_queries = docs_to_queries

    def accept(self, consumer_input: PipedInput):
        doc_id = consumer_input.get_doc_id()

        self.doc_features[doc_id] = {}
        for qid in self.docs_to_queries.get(doc_id, []):
            pair_key = "{}:{}".format(qid, doc_id)
            self.features[pair_key] = {}

    def dump(self):
        for qid in self.queries.keys():
            self.query_features[qid] = {}
