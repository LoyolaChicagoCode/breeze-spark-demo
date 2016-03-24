SCRIPT_HEADER="""#!/bin/bash

# This script is generated. Do not edit.

"""

SPARK_INIT="""

#
# Start Apache Spark on the allocated nodes
#

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

"""


SPARK_SUBMIT="""

#
# Submit the compiled Spark app to Spark
#

if [ -f "target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar" ]; then
	echo "Running: "$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URI \\
		target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar \\
		--dim %(dim)s --nodes %(nodes)s --partitions %(partitions)s --workload %(workload)s --outputdir /home/thiruvat/logs >> $JOB_LOG
	$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URI \\
		target/scala-2.10/demo-breeze-spark-scala-assembly-1.0.jar \\
		--dim %(dim)s --nodes %(nodes)s --partitions %(partitions)s --workload %(workload)s --outputdir /home/thiruvat/logs >> $JOB_LOG
else
	echo "Could not find Scala target diretory. No experiments run." >> $JOB_LOG
fi
"""

import os
import os.path

generated = os.path.join(".", "generated")
os.makedirs(generated)

GEN_FILENAME = "%(generated)s/run-n%(nodes)s-p%(partitions)s-w%(workload)s-dim%(dim)s.sh"

DIMENSIONS=[128, 256]
NODES=[4, 8, 16, 32, 64]
CORESPERNODE=12

for dim in DIMENSIONS:
   for nodes in NODES:
       partitions = nodes * CORESPERNODE
       workload = partitions
       filename = GEN_FILENAME % vars()
       with open(filename, "w") as outfile:
          outfile.write(SCRIPT_HEADER)
          outfile.write(SPARK_INIT)
          outfile.write(SPARK_SUBMIT % vars())

