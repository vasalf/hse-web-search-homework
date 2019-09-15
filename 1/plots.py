#!/usr/bin/python3

import json
import matplotlib.pyplot as plt


with open("results/extractor_stats.json", "r") as f:
    stats = json.load(f)

    plt.ylabel("Number of documents")
    plt.title("Document lengths in bytes")
    plt.hist(stats["docLengths"], bins=100, range=(0,40000))
    plt.savefig("doc_lengths.png")
    plt.clf()

    plt.ylabel("Number of documents")
    plt.title("Document lengths in words")
    plt.hist(stats["docWords"], bins=100, range=(0,6000))
    plt.savefig("doc_words.png")
    plt.clf()

    plt.ylabel("Number of documents")
    plt.title("Extracted bytes / bytes ratio")
    plt.hist(stats["docRatio"], bins=100)
    plt.savefig("doc_ratio.png")
    plt.clf()
