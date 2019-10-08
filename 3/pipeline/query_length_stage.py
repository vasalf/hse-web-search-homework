from pipeline.pipeline import PipelineStage, PipedInput


class QueryLengthStage(PipelineStage):
    def __init__(self, queries):
        self.queries = queries

    def accept(self, consumer_input: PipedInput):
        pass

    def dump(self):
        for query in self.queries:
            self.query_features[query.qid]["query_len"] = len(query.text)
