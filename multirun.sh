#!/bin/bash

masterHost=$1
peerNum=$2

python3 run.py $peerNum

ports=($(cat ports.txt))

for port in ${ports[@]}; do
    echo "./zrf -role raft -maddr $masterHost -saddr ${myhost}:${port}"
    ./zrf -role raft -maddr $masterHost -saddr ${myhost}:${port} -log 0 >log/${port}.log 2>&1 &
done
