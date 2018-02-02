#!/usr/bin/python

import sys
import os

import plotly.plotly as py
import plotly.graph_objs as go
import matplotlib.pyplot as plt

spark_home = os.environ.get('SPARK_HOME')
## file = sys.argv[1].rsplit('/', 1)[-1]
# path = spark_home + '/dags_exported/' + file
file = sys.argv[1]
path = spark_home + '/all_logs/policies/All'
SIZES = [1, 2, 3, 4, 5]
lib = 'graphx'

def find_last_job(data):
  job = -1
  for line in data:
    if '|| SIMULATION ||   jobid = ' in line:
      job = int(line.split('=')[1])
  return job
 

dict = {} 
dict[file]={}
for size in SIZES:
  pathfile = path + '/' + str(size) + '/' + lib + '/' + file
  print pathfile
  data = [line.strip() for line in open(pathfile, 'r')]
  last_job = find_last_job(data) 
  refound_last = False
  for line in data:
    if (not refound_last):
      if '|| SIMULATION ||   jobid = ' in line:
        job = int(line.split('=')[1])
        if (job == last_job):
          refound_last = True
    else:      
      if '|| SIMULATION ||   policy = ' in line:
        policy = line.split('= ')[1]
        if (size == SIZES[0]):
          dict[file][policy]={}
          dict[file][policy]['hits']=[]
          dict[file][policy]['misses']=[]
          dict[file][policy]['narrows']=[]
          dict[file][policy]['shuffles']=[]
      if '|| SIMULATION ||     hits' in line:
        hits = int(line.split('=')[1])
        dict[file][policy]['hits'].append(hits)
      if '|| SIMULATION ||     misses = ' in line:
        misses = int(line.split('=')[1])
        dict[file][policy]['misses'].append(misses)
      if '|| SIMULATION ||     diskHits = ' in line:
        diskHits = int(line.split('=')[1])
      if '|| SIMULATION ||     narrowDependencies = ' in line:
        narrows = int(line.split('=')[1])
        dict[file][policy]['narrows'].append(narrows)
      if '|| SIMULATION ||     shuffleDpendencies = ' in line:
        shuffles = int(line.split('=')[1])
        dict[file][policy]['shuffles'].append(shuffles)

print dict

policies = dict[file].keys
perf = ['hits', 'misses','narrows','shuffles']

for policy in ['LRU', 'LFU', 'FIFO', 'LRC', 'Belady']:
  for metr in perf:
    x1 = SIZES
    y1 = dict[file][policy][metr]
    print x1
    print y1
#    data = [go.Bar(x=x1, y=y1)]
    output = spark_home + '/diagrams/' + file + '_' + policy + '_' + metr
    plt.plot(x1, y1)
    plt.xlabel('Cache Size')
    plt.ylabel(metr)
    plt.savefig(output)
    plt.clf()

sys.exit()
 
