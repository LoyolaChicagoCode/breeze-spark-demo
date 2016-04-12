#!/usr/bin/env python2

import json
import sys
import os
import os.path
import pprint

# use argparse at some point
dir = sys.argv[1]

def read_performance_data(filename):
   with open(filename) as jsonfile:
      try:
         return json.loads(jsonfile.read())
      except:
         print("%s does not contain JSON data. Returning empty dict" % filename)
         return {}

class PerfData(object):
   # Keys from the JSON dictionary
   NODES = u'nodes'
   PARAMS = u'params'
   RESULTS = u'results'

   DIM = u'dim'
   PARTITIONS = u'partitions'
   WORKLOAD = u'workload'
   OUTPUT_DIR = u'outputDir'

   RDD_GENERATION_TIME = u'rdd generation time'
   RDD_REDUCE_TIME = u'rdd reduce time'

   RAW = u'raw'
   NS = u'nanoseconds'
   MS = u'milliseconds'

   def __init__(self, filename):
     self.data = {}
     with open(filename) as jsonfile:
      try:
         self.data = json.loads(jsonfile.read())
      except:
         print("%s does not contain JSON data. Returning empty dict" % filename)
         pass

   def nodes(self):
      return self.data[PerfData.NODES].keys()

   def node_counts(self):
      return self.data[PerfData.NODES].values()

   def dim(self):
      return self.data[PerfData.PARAMS][PerfData.DIM]

   def partitions(self):
      return self.data[PerfData.PARAMS][PerfData.PARTITIONS]

   def workload(self):
      return self.data[PerfData.PARAMS][PerfData.WORKLOAD]

   def output_dir(self):
      return self.data[PerfData.PARAMS][PerfData.OUTPUT_DIR]

   def rdd_generation_time(self, unit=MS):
      return self.data[PerfData.RESULTS][PerfData.RDD_GENERATION_TIME][unit]

   def rdd_reduce_time(self, unit=MS):
      return self.data[PerfData.RESULTS][PerfData.RDD_REDUCE_TIME][unit]
   
for file in os.listdir(dir):
   if file.endswith('.json'):
      path = os.path.join(dir, file)
      pd = PerfData(path)
 
      print(pd.nodes())
      print(pd.node_counts())
      print(pd.dim())
      print(pd.partitions())
      print(pd.output_dir())
      print(pd.rdd_generation_time())
      print(pd.rdd_reduce_time())
      print(pd.workload())

      





     
