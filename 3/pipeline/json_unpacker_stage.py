import json

from utils import *

from pipeline.pipeline import PipelineStage, PipedInput


class JsonUnpackerStage(PipelineStage):
    def accept(self, consumer_input: PipedInput):
        meta = json.loads(consumer_input.get_meta())
        return consumer_input.new(doc_id=meta["url"], meta=meta)

    def dump(self):
        pass
