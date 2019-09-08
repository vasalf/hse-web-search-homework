import nltk

nltk.download("stopwords")

from nltk.corpus import stopwords

from pipeline.pipeline import PipelineStage, PipedInput
from utils import *


class StopwordsCounter(PipelineStage):
    __russian_stopwords = stopwords.words("russian")
    __english_stopwords = stopwords.words("english")

    def __init__(self):
        self.russian_words = 0
        self.english_words = 0
        self.russian_stopwords = 0
        self.english_stopwords = 0

    def accept(self, consumer_input: PipedInput):
        for word in consumer_input.get_text():
            if is_russian(word):
                self.russian_words += 1
                if word in self.__russian_stopwords:
                    self.russian_stopwords += 1
            if is_english(word):
                self.english_words += 1
                if word in self.__english_stopwords:
                    self.english_stopwords += 1

        return consumer_input

    def dump(self):
        print(
            "Stopwords rate: total: {}, {} out of {}, russian: {}, {} out of {}, english: {}, {} out of {}"
                .format(
                (self.russian_stopwords + self.english_stopwords) / (self.russian_words + self.english_words),
                self.russian_stopwords + self.english_stopwords, self.russian_words + self.english_words,
                self.russian_stopwords / self.russian_words, self.russian_stopwords, self.russian_words,
                self.english_stopwords / self.english_words, self.english_stopwords, self.english_words))
