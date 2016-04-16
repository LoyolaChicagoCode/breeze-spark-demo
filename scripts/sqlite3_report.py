#!/usr/bin/env python2

# Parse XML performance data and write to an sqlite3 database for reporting.

import os
import os.path
import sys
import xml.etree.ElementTree as ET
import sqlite3

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
      return int(self.get_parameter('dim').attrib['value'])

   def partitions(self):
      return int(self.get_parameter('partitions').attrib['value'])

   def workload(self):
      return int(self.get_parameter('workload').attrib['value'])

   def nodes(self):
      return int(self.get_parameter('nodes').attrib['value'])

   def get_result(self, name):
       for result in self.root.findall('results/performance'):
         if result.attrib['name'] == name:
            return result
       return None

   def rdd_generation_time(self):
      return int(self.get_result("rdd generation time").find("time").attrib['nanoseconds'])

   def rdd_reduce_time(self):
      return int(self.get_result("rdd reduce time").find("time").attrib['nanoseconds'])

   def node_usage(self):
      node_info = {}
      for node in self.root.findall('nodes/node'):
         node_info[node.attrib['name']] = int(node.attrib['workload'])
      return node_info 
      
DROP_RESULTS_TABLE="DROP TABLE IF EXISTS results"

CREATE_RESULTS_TABLE="""CREATE TABLE IF NOT EXISTS results
  (id integer primary key, dim integer, partitions integer, workload integer, rdd_time integer,
   rdd_redue_time integer, nodes integer)
"""

INSERT_RESULT="INSERT INTO results VALUES (?,?,?,?,?,?,?)"

def next_id():
   id = 0
   while True:
     yield id
     id = id + 1

def write_results_db():
   conn = sqlite3.connect('breeze_results.db')
   cursor = conn.cursor()
   cursor.execute(DROP_RESULTS_TABLE)
   cursor.execute(CREATE_RESULTS_TABLE)

   dir = sys.argv[1]
   results = []
   id = next_id()
   for file in os.listdir(dir):
      if file.endswith('.xml'):
         print("Loading %s" % file)
         path = os.path.join(dir, file)
         pd = PerfDataXML(path)
         results.append( (id.next(), pd.dim(), pd.partitions(), pd.workload(), pd.rdd_generation_time(),
                        pd.rdd_reduce_time(),pd.nodes() ) )

   cursor.executemany(INSERT_RESULT, results)
   conn.commit()
   conn.close()

if __name__ == '__main__':
   write_results_db()
