#!/usr/bin/env python2

import os
import os.path
import sys
import xml.etree.ElementTree as ET

class PerfDataXML(object):
   def __init__(self, xml_filename):
      with open(xml_filename) as xmlf:
        self.root = ET.fromstring(xmlf.read())
      
   def get_parameter(self, name):
      for param in self.root.findall('parameters/param'):
         if param.attrib['name'] == name:
            return param
      return None

   def dim(self):
      return self.get_parameter('dim').attrib['value']

   def partitions(self):
      return self.get_parameter('partitions').attrib['value']

   def workload(self):
      return self.get_parameter('workload').attrib['value']

   def nodes(self):
      return self.get_parameter('nodes').attrib['value']

   def get_result(self, name):
       for result in self.root.findall('results/performance'):
         if result.attrib['name'] == name:
            return result
       return None

   def rdd_generation_time(self):
      return self.get_result("rdd generation time").find("time").attrib['nanoseconds']

   def rdd_reduce_time(self):
      return self.get_result("rdd reduce time").find("time").attrib['nanoseconds']

   def node_usage(self):
      node_info = {}
      for node in self.root.findall('nodes/node'):
         node_info[node.attrib['name']] = int(node.attrib['workload'])
      return node_info 
      

#
# TODO: Proof of concept. It works. Next stop: Generate CSV for analytics!
dir = sys.argv[1]
for file in os.listdir(dir):
   if file.endswith('.xml'):
      path = os.path.join(dir, file)
      pd = PerfDataXML(path)
      print(pd.dim())
      print(pd.partitions())
      print(pd.workload())
      print(pd.rdd_generation_time())
      print(pd.rdd_reduce_time())
      print("Nodes used")
      for (node, workload) in pd.node_usage().items():
         print("%(node)s = %(workload)d" % vars())
