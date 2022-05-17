#!/bin/python

import sys
import os

hosts = [
    "127.0.0.1"
]


def getPort():
    pscmd = "netstat -ntl | grep -v Active| grep -v Proto | awk '{print $4}' | awk -F: '{print $NF}'"
    procs = os.popen(pscmd).read()
    procarr = procs.split("\n")
    tt = random.randint(15000, 20000)
    if tt not in procarr:
        return tt
    else:
        getPort()


argv = sys.argv
if len(argv) < 2:
    raise Exception("argv.len < 2")
peerNum = argv[1]
print("peerNum={}".format(peerNum))
path = '.'
if len(argv) < 3:
    path = argv[2]
print("path={}".format(path))


# for i in range(peerNum):
