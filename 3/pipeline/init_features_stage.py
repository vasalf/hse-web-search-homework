from pipeline.pipeline import PipelineStage, PipedInput


class InitFeaturesStage(PipelineStage):
    def __init__(self, queries):
        self.queries = queries

    def accept(self, consumer_input: PipedInput):
        doc_id = consumer_input.get_doc_id()

        self.doc_features[doc_id] = {}
        for query in self.queries:
            pair_key = "{}:{}".format(query.qid, doc_id)
            self.features[pair_key] = {}

    def dump(self):
        for query in self.queries:
            self.query_features[query.qid] = {}
