#!/usr/bin/python

import sys
import os

## spark_home = os.environ.get('SPARK_HOME')
## file = sys.argv[1].rsplit('/', 1)[-1]
## path = spark_home + '/dags_exported/' + file
dir = sys.argv[1]
to_file = sys.argv[2]
list = os.listdir(dir)
for file in list:
  for line in open(file):
    if 'SIMULAT' in line:
      if 'hits' in line:
        hits = parse_misses(line)
        break
  file.close()
  
      

open(to_file,'w').writelines([ line for line in open(from_file) if 'DAGINFO' in line])
## os.remove(path)

