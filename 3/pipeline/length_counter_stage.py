from pipeline.pipeline import PipelineStage, PipedInput


class LengthCounterStage(PipelineStage):
    def accept(self, consumer_input: PipedInput):
        doc_id = consumer_input.get_doc_id()
        text = consumer_input.get_text()
        meta = consumer_input.get_meta()
        self.doc_features[doc_id]["text_len"] = len(text)
        self.doc_features[doc_id]["url_len"] = len(meta["url"])

    def dump(self):
        pass
