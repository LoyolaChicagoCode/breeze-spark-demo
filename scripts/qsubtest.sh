#!/bin/bash

cat $COBALT_NODEFILE >> ~/logs/qsubtest.$(hostname)
pwd >> ~/logs/qsubtest.$(hostname)

sleep 60
exit 0
