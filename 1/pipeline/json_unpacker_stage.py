import json

from utils import *

from pipeline.pipeline import PipelineStage, PipedInput


class JsonUnpackerStage(PipelineStage):
    def accept(self, consumer_input: PipedInput):
        return consumer_input.new(meta=json.loads(" ".join(consumer_input.get_meta())))

    def dump(self):
        pass
