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


def init_query_features(queries, query_features):
    for query in queries:
        query_features[query.qid] = {}


def shortest_window(query, document):
    q_words = set(query.split())
    d_words = document.split()
    nz = 0
    count = dict()
    j = 0
    for j in range(len(d_words)):
        if d_words[j] not in q_words:
            continue
        if d_words[j] not in count:
            count[d_words[j]] = 1
            nz += 1
        else:
            count[d_words[j]] += 1
        if nz == len(q_words):
            break
    if nz < len(q_words):
        return 0.0
    j += 1
    mn = j + 1
    for i in range(len(d_words)):
        if d_words[i] in q_words:
            count[d_words[i]] -= 1
            if count[d_words[i]] == 0:
                nz -= 1
        while j < len(d_words) and nz < len(q_words):
            if d_words[j] not in q_words:
                j += 1
                continue
            count[d_words[j]] += 1
            if count[d_words[j]] == 1:
                nz += 1
            j += 1
        if nz == len(q_words):
            mn = min(mn, j - i)
    return len(q_words) / (mn - 1)


def fraction_of_words(query, document):
    q_words = set(query.split())
    present = set()
    for w in document.split():
        if w not in q_words:
            continue
        present.add(w)
    return len(present) / len(q_words)
