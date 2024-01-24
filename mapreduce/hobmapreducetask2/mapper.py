#!/usr/bin/env python

import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')

pattern = re.compile("\[\d\d\d\d-\d\d-\d\d.\d\d:\d\d:\d\d\]")

for line in sys.stdin:
    try:
        line = unicode(line)
    except ValueError as e:
        continue
    if pattern.match(line[:21]):
        if "UUID of player " in line:
            username = line.split("UUID of player ")[1].split(" ")[0]
            print "%s\t%d" % (username, 0)
        elif " died" in line:
            username = line.split(" died")[0].split(" ")[-1]
            print "%s\t%d" % (username, 1)
        elif " lost connection: Disconnected" in line:
            username = line.split(" lost connection: Disconnected")[0].split(" ")[-1]
            print "%s\t%d" % (username, 2)
