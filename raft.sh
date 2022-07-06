#!/bin/bash

maddr=$1
./zrf -role raft -maddr 219.228.148.${maddr}:23332 -saddr ${myhost}:23333
