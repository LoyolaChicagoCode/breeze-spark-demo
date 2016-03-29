import argparse
import sys
import os
import os.path
import tempfile



parser = argparse.ArgumentParser()
parser.add_argument('--dims', type=int, nargs='+', default=[128, 256], help='dimension of 3D array (be careful)')
parser.add_argument('--nodes', type=int, nargs='+', default=[ 2**n for n in range(0, 6) ], help="cluster nodes (used for naming scripts only)")
parser.add_argument('--cores', type=int, nargs='+', default=[12], help="cores per node (used to set partitions on Spark)")
parser.add_argument('--add_workload', type=int, nargs='+', default=[], help="add a fixed workload (used for weak vs. strong scaling)")
parser.add_argument('--basedir', type=type(''), default=os.getcwd(), help="where to write the scripts (current working directory")
options = parser.parse_args()

import tempfile
generated = tempfile.mkdtemp('.d', 'qsubs.', options.basedir)
print("Scripts will be written to dir %s " % generated)

from cooley_include import *

for dim in options.dims:
   for nodes in options.nodes:
      for cores in options.cores:
        partitions = nodes * cores
        workload = [partitions] + options.add_workload
	add_workload_text = ", ".join([ str(item) for item in options.add_workload])
        all_workloads = list(set(workload))
        for workload in all_workloads:
           filename = SCRIPT_FILENAME % vars()
           script_code = SCRIPT_HEADER + SPARK_INIT + SPARK_SUBMIT
           with open(filename, "w") as outfile:
               os.chmod(filename, 0o755)
               outfile.write(script_code % vars())

