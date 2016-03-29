# For this to work in your environment, you'll have to specify these variables
# on the command line to override.

SHELL=/bin/bash

# Apache Spark default setup
SPARK_HOME=/home/thiruvat/code/spark
SPARK_MASTER_URI=spark://hostname

# Experimental Setup
DIM=128
NODES=4
PARTITIONS=48
WORKLOAD=48
LOGDIR=/home/thiruvat/logs

all:
	sbt compile

assembly:
	sbt assembly

slowclean:
	sbt clean
	find . -type d -name target -print | xargs rm -rf
	find . -type d -name '*.d' -print | xargs rm -rf

fastclean:
	rm -f *.error *.cobaltlog *.output
	rm -f ~/logs/*

clean:
	make slowclean
	make fastclean

#
# Note: These are rather specific to my environment.
# Try make submit if you have your own cluster.

qsub_scripts:
	./scripts/cooley.sh
