import re

russian = "абвгдеёжзийклмнопрстуфхцчшщьыъэюя"
belarusian = "абвгдеёжзiйклмнопрстуўфхцчшьыэюя"
english = "abcdefghijklmnopqrstuvwxyzéôïç"

russian_re = re.compile("^[{}]+$".format(russian))
belarusian_re = re.compile("^[{}]+$".format(belarusian))
english_re = re.compile("^[{}]+$".format(english))


def is_russian(word):
    return russian_re.search(word) is not None


def is_belarusian(word):
    return belarusian_re.search(word) is not None


def is_english(word):
    return english_re.search(word) is not None
