#!/bin/bash

# Start Apache Spark (ephermally) at run experiments.
# This will be replaced by code to generate the experiments


JOB_LOG=$HOME/logs/log.$COBALT_JOBID

echo "starting job $COBALT_JOBID" > $JOB_LOG

echo "starting spark..." >> $JOB_LOG
pushd $HOME/code/spark
cat $COBALT_NODEFILE > conf/slaves
cat $COBALT_NODEFILE >> $JOB_LOG
./sbin/start-all.sh
echo "spark started..." >> $JOB_LOG
NODES=`wc -l conf/slaves | cut -d" " -f1`
popd

MASTER=`hostname`

echo "# Spark is now running with $NODES workers:" >> $JOB_LOG
echo "export SPARK_STATUS_URL=http://$MASTER.cooley.pub.alcf.anl.gov:8000" >> $JOB_LOG
echo "export SPARK_MASTER_URI=spark://$MASTER:7077" >> $JOB_LOG

SPARK_MASTER_URI=spark://$MASTER:7077
SPARK_HOME=$HOME/code/spark

CORESPERNODE=12
DIM=128
PARTITIONS=$(($NODES * $CORESPERNODE))
WORKLOAD=$PARTITIONS  # for now
LOGDIR=/home/thiruvat/logs

if [ -d target ]; then
	echo "Running: "$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URI \
		target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar \
		--dim $DIM --nodes $NODES --partitions $PARTITIONS --workload $WORKLOAD --outputdir $LOGDIR >> $JOB_LOG
	$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URI \
		target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar \
		--dim $DIM --nodes $NODES --partitions $PARTITIONS --workload $WORKLOAD --outputdir $LOGDIR
	echo "Done running Spark job" >> $JOB_LOG
	$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URI \
		target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar \
		--dim $DIM --nodes $NODES --partitions $PARTITIONS --workload $WORKLOAD --outputdir $LOGDIR
	echo "Done running Spark job" >> $JOB_LOG
else
	echo "Could not find Scala target diretory. No experiments run." >> $JOB_LOG
fi
