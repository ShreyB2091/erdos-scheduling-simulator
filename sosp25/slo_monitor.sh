#!/bin/bash

for arg in "$@"
do
    echo "$arg"
    rg "LOG_STATS" "$arg" | tail
    echo "-----------------------"
done
