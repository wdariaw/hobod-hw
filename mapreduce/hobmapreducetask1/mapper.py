#!/usr/bin/env python

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')


for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue

    word_list = re.split('\W*\s+\W*', text, re.UNICODE)
    for word in word_list:
        if len(word) >= 6 and len(word) <= 9:
            if word[0].isupper() and word[1:] == word[1:].lower():
                print "%s\t%d" % (word.lower(), 0)
            else:
                print "%s\t%d" % (word.lower(), -1)
