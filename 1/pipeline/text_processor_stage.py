from pymystem3 import Mystem

from utils import *

from pipeline.pipeline import PipelineStage, PipedInput


class TextProcessorStage(PipelineStage):
    def __init__(self):
        self.mystem = Mystem()

    def accept(self, consumer_input: PipedInput):
        tokens = self.mystem.lemmatize(" ".join(consumer_input.get_text()).lower())
        result = []
        for token in tokens:
            token = token.strip()
            if is_russian(token) or is_belarusian(token) or is_english(token):
                result.append(token)

        return consumer_input.new(text=result)

    def dump(self):
        pass
