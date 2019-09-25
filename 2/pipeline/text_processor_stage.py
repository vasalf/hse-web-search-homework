import sys

from pymystem3 import Mystem
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import PorterStemmer
from nltk.stem.snowball import RussianStemmer
import json
from utils import *

from pipeline.pipeline import PipelineStage, PipedInput


class TextLemmatizerStage(PipelineStage):
    def __init__(self):
        self.mystem = Mystem()

    def accept(self, consumer_input: PipedInput):
        tokens = self.mystem.lemmatize(consumer_input.get_text().lower())
        result = []
        for token in tokens:
            token = token.strip()
            if is_russian(token) or is_belarusian(token) or is_english(token):
                result.append(token)
        return consumer_input.new(text=" ".join(result))

    def dump(self):
        pass


class TextStemmerStage(PipelineStage):
    def __init__(self):
        self.english_stemmer = PorterStemmer()
        self.russian_stemmer = RussianStemmer()

    def accept(self, consumer_input: PipedInput):
        text = consumer_input.get_text().lower()
        token_words = word_tokenize(text)
        result = []
        for token in token_words:
            token = token.strip()
            if is_russian(token) or is_belarusian(token):
                result.append(self.russian_stemmer.stem(token))
            if is_english(token):
                result.append(self.english_stemmer.stem(token))
        return consumer_input.new(text=" ".join(result))

    def dump(self):
        pass


class TextWithHeaderStage(PipelineStage):
    def accept(self, consumer_input: PipedInput):
        text = consumer_input.get_meta()['title'] + " . " + consumer_input.get_text()
        return consumer_input.new(text=text, meta=json.dumps(consumer_input.get_meta()))

    def dump(self):
        pass
