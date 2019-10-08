from pipeline.pipeline import PipelineStage, PipedInput
from math import log2


class BM25Stage(PipelineStage):
    def __init__(self, queries):
        self.df = {}
        self.tf = {}
        self.queries = queries
        for query in queries:
            for word in query.text.split():
                self.df[word] = 0
                self.tf[word] = {}
        self.doc_lengths = {}

    def accept(self, consumer_input: PipedInput):
        text = consumer_input.get_text()
        doc_id = consumer_input.get_doc_id()

        self.doc_lengths[doc_id] = len(text)

        for word in self.df.keys():
            if word in text:
                self.df[word] += 1
                if doc_id in self.tf[word]:
                    self.tf[word][doc_id] += 1
                else:
                    self.tf[word][doc_id] = 1

    def dump(self):
        K1 = 2.0
        B = 0.75
        doc_n = len(self.doc_lengths)
        avgdl = sum(self.doc_lengths.values()) / doc_n

        idf = {}
        for word, df in self.df.items():
            if df == 0:
                idf[word] = 0
            else:
                idf[word] = log2(doc_n / df)

        for doc_id, doc_len in self.doc_lengths.items():
            for query in self.queries:
                score = 0
                for word in query.text.split():
                    tf = self.tf[word].get(doc_id, 0)
                    score += idf[word] * \
                             (tf * (K1 + 1)) / \
                             (tf + K1 * (1 - B + B * doc_len / avgdl))
                self.features["{}:{}".format(query.qid, doc_id)]["BM25"] = score
