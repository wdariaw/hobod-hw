#!/usr/bin/env python

import sys


prev_key = None

word_count = 0
balance = 0

for line in sys.stdin:
    try:
        key, num = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    if prev_key != key:
        if prev_key and balance == 0:
            print "%s\t%d" % (prev_key, word_count)
        
        # new word started
        word_count = 0
        balance = 0
        prev_key = key

    word_count += 1
    balance += int(num)

if prev_key and balance == 0:
    print "%s\t%d" % (prev_key, word_count)
