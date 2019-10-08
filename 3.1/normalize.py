#!/usr/bin/python3

import re
import sys

N_FEATURES = 245
INT_FEATURE_RE = re.compile("(\d+):(.*)")

s = sys.stdin.readline()
while s != "":
    int_features = [0.0 for i in range(N_FEATURES)]
    printed = False
    for data in s.split():
        m = INT_FEATURE_RE.match(data)
        if m:
            int_features[int(m.groups()[0]) - 1] = float(m.groups()[1])
    for data in s.split():
        if INT_FEATURE_RE.match(data):
            if not printed:
                for i, x in enumerate(int_features):
                    print("{}:{:>.6f}".format(i + 1, x), end=" ")
                printed = True
        else:
            print(data, end=" ")
    print()
    s = sys.stdin.readline()

