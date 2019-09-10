import math
import os
import time

import nltk
import operator

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

    def dump(self):
        print(
            "Stopwords rate: total: {}, {} out of {}, russian: {}, {} out of {}, english: {}, {} out of {}"
                .format(
                (self.russian_stopwords + self.english_stopwords) / (self.russian_words + self.english_words),
                self.russian_stopwords + self.english_stopwords, self.russian_words + self.english_words,
                self.russian_stopwords / self.russian_words, self.russian_stopwords, self.russian_words,
                self.english_stopwords / self.english_words, self.english_stopwords, self.english_words))


class SimpleStats(PipelineStage):
    def __init__(self):
        self.not_english_words = 0
        self.english_words = 0
        self.english_lengths_sum = 0
        self.not_english_lengths_sum = 0

    def accept(self, consumer_input: PipedInput):
        for word in consumer_input.get_text():
            if is_english(word):
                self.english_words += 1
                self.english_lengths_sum += len(word)
            else:
                self.not_english_words += 1
                self.not_english_lengths_sum += len(word)

    def dump(self):
        print("English words rate: {}, {} out of {}"
              .format(self.english_words / (self.english_words + self.not_english_words), self.english_words,
                      (self.english_words + self.not_english_words)))
        print("Average word length: all words: {}, (belo)russian: {}, english: {}."
              .format(
            (self.english_lengths_sum + self.not_english_lengths_sum) / (self.english_words + self.not_english_words),
            self.not_english_lengths_sum / self.not_english_words,
            self.english_lengths_sum / self.english_words))


class DictionaryStats(PipelineStage):
    __TOP_FREQUENCIES = 10000
    __TOP_IDFS = 10000

    def __init__(self, timestamp):
        self.timestamp = timestamp
        self.tf = {}
        self.documents_count = 0
        self.idf = {}

    def accept(self, consumer_input: PipedInput):
        for word in set(consumer_input.get_text()):
            self.idf[word] = self.idf.get(word, 0) + 1
        for word in consumer_input.get_text():
            self.tf[word] = self.tf.get(word, 0) + 1
        self.documents_count += 1

    def dump(self):
        sorted_frequencies = sorted(self.tf.items(), key=operator.itemgetter(1), reverse=True)
        idfs = sorted([(k, math.log(self.documents_count / v)) for k, v in self.idf.items()],
                      key=operator.itemgetter(1), reverse=True)[:self.__TOP_IDFS]

        with open(os.path.join("results", "top_frequent_words_{}.txt".format(self.timestamp)), 'w') as file:
            file.writelines(map(lambda x: "{} {}\n".format(x[0], x[1]), sorted_frequencies[:self.__TOP_FREQUENCIES]))
        with open(os.path.join("results", "top_idf_words_{}.txt".format(self.timestamp)), 'w') as file:
            file.writelines(map(lambda x: "{} {}\n".format(x[0], x[1]), idfs[:self.__TOP_IDFS]))
