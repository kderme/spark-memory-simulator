#!/usr/bin/python

import sys
import os

## spark_home = os.environ.get('SPARK_HOME')
## file = sys.argv[1].rsplit('/', 1)[-1]
# path = spark_home + '/dags_exported/' + file
path = sys.argv[1]
#to_file = sys.argv[2]
dictionary = {}
for file in os.listdir(path):
  n = 0
  for line in open(path + '/' + file):
    if '|| SIMULATION || hits = ' in line:
      hits = int(line.split('=')[1])
      n = n + hits
  print(file)
  print n
  if (n != 0):
    dictionary[file] = n
print(dictionary)
