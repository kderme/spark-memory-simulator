#!/usr/bin/python

import sys
import os
import json

## import plotly.plotly as py
## import plotly.graph_objs as go
import matplotlib.pyplot as plt
import numpy as np
## import ntpath

Metrics = ['hits', 'misses', 'narrow dependencies', 'shuffled dependencies']
Base = '/home/kostas/Repos/spark-logging-dag/diagrams/5'

def whatisthis(s):
    if isinstance(s, str):
        print "ordinary string"
    elif isinstance(s, unicode):
        print "unicode string"
    else:
        print "not a string"

def toJsonFile (file):
  data = [line.strip() for line in open(file, 'r')]
  newLines = []
  for line in data:
    if '|| SIMULATION ||' in line:
      print line
      newLines.append(line.split('||')[2])
  print newLines
  output = file + '.json'
  outfile = open(output, 'w')
  for line in newLines:
    print>>outfile, line
  return output

def toDict(inputDict):
  dict = {}
  lastJob = inputDict['Final Job Id']
  app = inputDict['appName'].encode('utf-8')
  dict[app] = {}
  simulations = inputDict['simulations']
  for simulation in simulations:
    if simulation['valid'] == False or simulation['jobid'] < lastJob:
      continue
    policy = simulation['policy']
    size = simulation['memory capacity']
    if (not bool(dict[app].get(policy))):
      dict[app][policy]={}
      for m in Metrics:
        dict[app][policy][m] = {}
    for m in Metrics:
      dict[app][policy][m][size] = simulation[m]
  return dict

def unzip(tuples):
  keys = tuples.keys()
  vals = []
  for k in keys:
    vals.append(tuples[k])
  return keys, vals

def toGraphs(dict):
  app = dict.keys()[0].encode('ascii', 'ignore')
  dict1 = dict[dict.keys()[0]]
  policies = dict1.keys()
  for policy in policies:
    dict2 = dict1[policy]
    for metric in Metrics:
      tuples = dict2[metric]
      x1, x2 = unzip(tuples)
      app_name = app.replace(" ", "")
      directory = Base + '/' + app_name
      output = directory + '/' + app_name + '_' + policy + '_' + metric
      plt.plot(x1, x2)
      plt.xlabel('Cache Size')
      plt.ylabel(metric)   
      if not os.path.exists(directory):
        os.makedirs(directory)
      plt.savefig(output)
      plt.clf()
    metric = 'hit-ratio'
    x,x2 = unzip(dict2['hits'])
    y1,y2 = unzip(dict2['misses'])
    vals = zip(x2,y2)
    y = map(lambda hm: hm[0]*1.0/((hm[0]+hm[1])*1.0), vals)
    app_name = app.replace(" ", "")
    directory = Base + '/' + app_name
    output =  directory + '/ ' + app_name + '_' + policy + '_' + metric
    plt.plot(x, y)
    plt.xlabel('Cache Size')
    plt.ylabel(metric)
    plt.savefig(output)  
    plt.clf()
 
def toGraphs2(dict):
  app = dict.keys()[0]
  app_name = app.replace(" ", "")
  dict1 = dict[dict.keys()[0]]
  policies = dict1.keys()
  for metric in Metrics:
##    plt.gca().set_color_cycle(['red', 'green', 'blue', 'yellow'])
    for policy in policies:
      tuples = dict1[policy][metric]
      x1, x2 = unzip(tuples)
      plt.plot(x1,x2)
    plt.legend(policies, loc='upper left')
    directory = Base + '/' + app_name  
    output = directory + '/' + app_name + '_' + metric
    if not os.path.exists(directory):
      os.makedirs(directory)
    plt.savefig(output)
    plt.clf()

  metric = 'hit-ratio'
  for policy in policies:
    x,x2 = unzip(dict1[policy]['hits'])
    y1,y2 = unzip(dict1[policy]['misses'])
    vals = zip(x2,y2)
    y = map(lambda hm: hm[0]*1.0/((hm[0]+hm[1])*1.0), vals)
    plt.plot(x, y)
    plt.legend(policies, loc='upper left')
  output = Base + '/' + app_name + '/' + app_name + '_' + metric
  plt.savefig(output)
  plt.clf()

## spark_home = os.environ.get('SPARK_HOME')
for file in sys.argv[1:]:
  json_file = open(toJsonFile(file))
  inputDict = json.load(json_file)
  dict = toDict(inputDict)
  toGraphs(dict)
  toGraphs2(dict)

sys.exit()
os.exit
exit
file = sys.argv[1]
js = toJson (file)
dic = json.load(open(js))
data = [line.strip() for line in open(file, 'r')]
last_job = find_last_job(data)
print dic[0]["valid"]
print range(len(dic))
dic2 = []
for d in dic:
    print d
    if bool(d.get("jobid")) and d["jobid"] == str(last_job) and bool(d.get("valid") == True):
        dic2.append(d)
open(file + ".json", "w").write(
    json.dumps(dic2, sort_keys=True, indent=4, separators=(',', ': '))
)

sizes = []
for i in range(len(dic2)):
  trial = dic2[i]
  policy = trial['policy']
  hits = int(trial['hits'])
  misses = int(trial['misses'])
  narrows = int(trial['narrowDependencies'])
  shuffles = int(trial['shuffleDpendencies'])
  size = int(trial['memory capacity'])
  if not size in sizes:
    sizes.append(size)
  if (not bool(dict[file].get(policy))):
    dict[file][policy]={}
    dict[file][policy]['hits']=[]
    dict[file][policy]['misses']=[]
    dict[file][policy]['narrows']=[]
    dict[file][policy]['shuffles']=[]
  dict[file][policy]['hits'].append(hits)
  dict[file][policy]['misses'].append(misses)
  dict[file][policy]['narrows'].append(narrows)
  dict[file][policy]['shuffles'].append(shuffles)
print dict
print sizes

policies = dict[file].keys
perf = ['hits', 'misses','narrows','shuffles']

for policy in ['LRU', 'LFU', 'FIFO', 'LRC', 'Belady']:
  for metr in perf:
    x1 = sizes
    y1 = dict[file][policy][metr]
    print x1
    print y1
#    data = [go.Bar(x=x1, y=y1)]
    name = ntpath.basename(file)
    output = '/home/kostas/Repos/spark-logging-dag/diagrams/4/' + name + '_' + policy + '_' + metr
    plt.plot(x1, y1)
    plt.xlabel('Cache Size')
    plt.ylabel(metr)
    plt.savefig(output)
    plt.clf()

sys.exit()
## for size in SIZES:
##   pathfile = path + '/' + str(size) + '/' + lib + '/' + file
##   print pathfile
data = [line.strip() for line in open(pathfile, 'r')]
last_job = find_last_job(data)
while true:
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
      if '|| SIMULATION ||   memory capacity' in line:
        size = int(line.split('=')[1])
        if (bool(dict[file].get(policy))):
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


