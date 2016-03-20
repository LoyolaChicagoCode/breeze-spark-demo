# For this to work in your environment, you'll have to specify these variables
# on the command line to override.

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

fastclean:
	rm -f *.error *.cobaltlog *.output
	rm -f ~/logs/*

clean:
	make slowclean
	make fastclean

#
# Note: These are rather specific to my environment.
# Try make submit if you have your own cluster.

submit:
	$(SPARK_HOME)/bin/spark-submit --master $(SPARK_MASTER_URI) target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar --dim $(DIM) --nodes $(NODES) --partitions $(PARTITIONS) --workload $(WORKLOAD) --outputdir $(LOGDIR)

cooley:
	qsub -n $(NODES) -t 00:15:00 -A ExaHDF5 -q pubnet ./scripts/run-cooley.sh
