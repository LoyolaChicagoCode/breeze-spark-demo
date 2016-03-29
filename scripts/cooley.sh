#!/bin/bash

# This script is usually run in the same folder as the top-level Makefile
# You can run it from the top-level or the scripts/ folder.

if [ -f scripts/cooley.py ]; then
	export DIR=$(pwd)/scripts
	export OUTDIR=$(pwd)
fi

if [ -f cooley.py ]; then
	export DIR=$(pwd)
	export OUTDIR=..
fi

export PYTHONPATH=$DIR:$PYTHONPATH

python $DIR/cooley.py --dims 128 256 512 1024 2048 --nodes 4 8 16 32 64 120 --cores 12 --add_workload $(expr 120 \* 12) --basedir $OUTDIR
