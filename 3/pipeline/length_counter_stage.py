import json
import os
from pipeline.pipeline import PipelineStage, PipedInput


class LengthCounterStage(PipelineStage):
    def __init__(self, outname="lengths"):
        self.lengths = {}
        self.outname = outname

    def accept(self, consumer_input: PipedInput):
        doc_id = consumer_input.get_doc_id()
        text = consumer_input.get_text()
        meta = consumer_input.get_meta()

        self.lengths[doc_id] = {"text_len": len(text), "url_len": len(meta["url"])}

    def dump(self):
        with open(os.path.join("results", self.outname + ".json"), 'w') as file:
            json.dump(self.lengths, file)
