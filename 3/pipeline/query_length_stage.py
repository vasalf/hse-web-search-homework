from pipeline.pipeline import PipelineStage, PipedInput


class QueryLengthStage(PipelineStage):
    def __init__(self, queries):
        self.queries = queries

    def accept(self, consumer_input: PipedInput):
        pass

    def dump(self):
        for qid, text in self.queries.items():
            self.query_features[qid]["query_len"] = len(text)
