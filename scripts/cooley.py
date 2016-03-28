import argparse
import sys
import os
import os.path
import cooley_include as templates

generated = os.path.join("..", "generated")
if os.path.exists(generated):
   print("error: %s exists. Please delete and re-run." % generated)
   sys.exit(1)

os.makedirs(generated)
parser = argparse.ArgumentParser()
parser.add_argument('--dims', type=int, nargs='+', default=[128, 256])
parser.add_argument('--nodes', type=int, nargs='+', default=[ 2**n for n in range(0, 6) ])
parser.add_argument('--cores', type=int, nargs='+', default=[12])
options = parser.parse_args()

GEN_FILENAME = "%(generated)s/run-n%(nodes)s-p%(partitions)s-w%(workload)s-dim%(dim)s.sh"

for dim in options.dims:
   for nodes in options.nodes:
      for cores in options.cores:
        partitions = nodes * cores
        workload = partitions
        filename = GEN_FILENAME % vars()
        with open(filename, "w") as outfile:
            print("Creating script %s" % filename)
            outfile.write(templates.SCRIPT_HEADER)
            outfile.write(templates.SPARK_INIT)
            outfile.write(templates.SPARK_SUBMIT % vars())

