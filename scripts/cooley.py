import argparse
import sys
import os
import os.path
import tempfile


import tempfile
generated = tempfile.mkdtemp('.d', 'qsubs.', os.getcwd())
print("Scripts will be written to dir %s " % generated)

parser = argparse.ArgumentParser()
parser.add_argument('--dims', type=int, nargs='+', default=[128, 256])
parser.add_argument('--nodes', type=int, nargs='+', default=[ 2**n for n in range(0, 6) ])
parser.add_argument('--cores', type=int, nargs='+', default=[12])
parser.add_argument('--add_workload', type=int, nargs='+', default=[])
options = parser.parse_args()

from cooley_include import *

for dim in options.dims:
   for nodes in options.nodes:
      for cores in options.cores:
        partitions = nodes * cores
        workload = partitions
        all_workloads = [ workload ] + options.add_workload
        for workload in all_workloads:
           filename = SCRIPT_FILENAME % vars()
           script_code = SCRIPT_HEADER + SPARK_INIT + SPARK_SUBMIT
           with open(filename, "w") as outfile:
               os.chmod(filename, 0o755)
               outfile.write(script_code % vars())

