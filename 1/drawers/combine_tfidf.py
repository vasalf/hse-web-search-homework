#!/usr/bin/env python3
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--frequencies", type=str, help="Directory to read word frequencies from",
                        default="top_frequent_words.txt")
    parser.add_argument('-i', "--idfs", type=str, help="Directory to read word idfs from",
                        default="top_idf_words.txt")

    args = parser.parse_args()

    fs = {}
    with open(args.frequencies, 'r') as f:
        for line in f.readlines():
            word = line.split()[0]
            freq = float(line.split()[1])
            fs[word] = freq

    idfs = {}
    with open(args.idfs, 'r') as f:
        for line in f.readlines():
            word = line.split()[0]
            freq = float(line.split()[1])
            idfs[word] = freq

    result = []

    for word, idf in idfs.items():
        result.append((word, idf * fs[word]))

    with open('top_tf-idf.txt', 'w') as f:
        for word, val in sorted(result, key=lambda x: x[1], reverse=True):
            f.write("{} {}\n".format(word, val))


if __name__ == "__main__":
    main()
