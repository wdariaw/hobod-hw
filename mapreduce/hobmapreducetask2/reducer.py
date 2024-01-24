#!/usr/bin/env python

import sys


prev_key = None

dies_count = 0
sessions_count = 0
total = 0

for line in sys.stdin:
    try:
        key, num = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    if prev_key != key:
        if prev_key and total > 0:
            print "%s\t%.2f\t%d" % (prev_key, dies_count * 1.0 / sessions_count, sessions_count)
        total = 0
        sessions_count = 0
        dies_count = 0
        prev_key = key

    if num == "0":
        sessions_count += 1
    if num == "1":
        dies_count += 1
    total += 1

if prev_key:
    print "%s\t%.2f\t%d" % (prev_key, dies_count * 1.0 / sessions_count, sessions_count)
