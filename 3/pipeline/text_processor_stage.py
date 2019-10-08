from copy import copy

import nltk
from nltk.corpus import stopwords
from pymystem3 import Mystem

from utils import *

from pipeline.pipeline import PipelineStage, PipedInput

nltk.download("stopwords")
mystem = Mystem()
russian_stopwords = stopwords.words("russian")
english_stopwords = stopwords.words("english")


class TextProcessorStage(PipelineStage):
    @staticmethod
    def lemmatize(text):
        tokens = mystem.lemmatize(" ".join(text).lower())
        result = []
        for token in tokens:
            token = token.strip()
            if is_russian(token) or is_belarusian(token) or is_english(token):
                result.append(token)
        return " ".join(result)

    def accept(self, consumer_input: PipedInput):
        new_meta = copy(consumer_input.get_meta())
        new_meta["title"] = self.lemmatize(new_meta["title"])
        return consumer_input.new(text=self.lemmatize(consumer_input.get_text()), meta=new_meta)

    def dump(self):
        pass


class StopwordsFilter(PipelineStage):

    @staticmethod
    def filter_stopwords(text):
        result = []
        for word in text:
            if is_russian(word) and word not in russian_stopwords \
                    or is_english(word) and word not in english_stopwords:
                result.append(word)
        return " ".join(result)

    def accept(self, consumer_input: PipedInput):
        new_meta = copy(consumer_input.get_meta())
        new_meta["title"] = self.filter_stopwords(new_meta["title"])
        return consumer_input.new(text=self.filter_stopwords(consumer_input.get_text()), meta=new_meta)
