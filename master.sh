#!/bin/bash

peerNum=$1
./zrf -role master -maddr ${myhost}:23332 -n $peerNum
