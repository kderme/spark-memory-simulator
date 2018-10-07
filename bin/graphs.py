#!/usr/bin/python

import sys
import os
import json
import copy
from ntpath import basename
from shutil import copyfile

## import plotly.plotly as py
## import plotly.graph_objs as go
import matplotlib.pyplot as plt
import numpy as np
## import ntpath

Metrics = ['hits', 'misses', 'narrow dependencies', 'shuffled dependencies']
GraphsDir = '/home/kostas/Repos/spark-logging-dag/graphs/'

## Base = '/home/kostas/Repos/spark-logging-dag/graphs/fine_parts-1_size-8'

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
  memory = {}
  for line in data:
    if '|| SIMULATION ||' in line:
      print line
      newLines.append(line.split('||')[2])
    if '|| Memory:' in line:
      mems = line.split('||')
      id = int(mems[1].split(':')[1])
      if (not bool(memory.get(id))):
        memory[id] = []
      memory[id].append(mems[2])
  print newLines
  output = file + '.json'
  outfile = open(output, 'w')
  for line in newLines:
    print>>outfile, line
  return output, memory

def toDict(inputDict):
  dict = {}
  lastJob = inputDict['Final Job Id']
  app = inputDict['appName'].encode('utf-8')
  dict[app] = {}
  confs = inputDict['simulations conf']
  simulations = inputDict['simulations']
  for simulation in simulations:
    if simulation['valid'] == False or simulation['jobid'] < lastJob:
      continue
    id = str(simulation['simulation id'])
    scheduler = confs[id]['scheduler']
    predictor = confs[id]['size predictor']
    size = int(confs[id]['memory capacity'])
    policy = confs[id]['policy']
    if (not bool(dict[app].get(scheduler))):
      dict[app][scheduler]={}
    if (not bool(dict[app][scheduler].get(predictor))):
      dict[app][scheduler][predictor]={}
    d = dict[app][scheduler][predictor]
    if (not bool(d.get(policy))):
      d[policy]={}
      for m in Metrics:
        d[policy][m] = {}
    for m in Metrics:
      d[policy][m][size] = simulation[m]
  return dict, confs

def unzip(tuples):
  keys = tuples.keys()
  keys.sort()
  vals = []
  for k in keys:
    vals.append(tuples[k])
  return keys, vals

def predictorToTitle(predictor):
  if predictor == 'easy':
     return '1x1'
  else:
     x = predictor.split('-')[1]
     y = predictor.split('-')[2]
     return x + 'x' + y
     
def toGraphs(dict):
  app = dict.keys()[0].encode('ascii', 'ignore')
  app_name = app.replace(" ", "")
  for scheduler in dict[app].keys():
    for predictor in dict[app][scheduler].keys():
      for policy in dict[app][scheduler][predictor].keys():
        dict2 = dict[app][scheduler][predictor][policy]
        for metric in Metrics:
          tuples = dict2[metric]
          x1, x2 = unzip(tuples)
          directory = Base + '/' + app_name + '/verbose'
          output = directory + '/' + policy + '_' + metric + '_' + predictorToTitle(predictor) + '_ ' + scheduler + '.png'
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
        directory = Base + '/' + app_name + '/verbose'
        output = directory + '/' + policy + '_' + metric + '_' + predictorToTitle(predictor) + '_ ' + scheduler + '.png'
        plt.plot(x, y)
        plt.xlabel('Cache Size')
        plt.ylabel(metric)
        plt.savefig(output)  
        plt.clf()

def toGraphs2(dict):
  app = dict.keys()[0].encode('ascii', 'ignore')
  app_name = app.replace(" ", "")
  for scheduler in dict[app].keys():
    for predictor in dict[app][scheduler].keys():
      dict1 = dict[app][scheduler][predictor]
      policies = dict1.keys()
      for metric in Metrics:
#        plt.gca().set_color_cycle(['red', 'green', 'yellow', 'blue'])
        for policy in policies:
          tuples = dict1[policy][metric]
          x1, x2 = unzip(tuples)
          plt.plot(x1,x2)
        plt.legend(policies, loc='upper right')
        plt.xlabel('Cache Size')
        plt.ylabel(metric)
        plt.title(app_name + ' ' + predictorToTitle(predictor))
        directory = Base + '/' + app_name  
        output = directory + '/' + metric + '_' + predictorToTitle(predictor) + '_ ' + scheduler + '.png'
        if not os.path.exists(directory):
          os.makedirs(directory)
        plt.savefig(output)
        plt.clf()

      metric = 'hit-ratio'
#      plt.gca().set_color_cycle(['red', 'green', 'yellow', 'blue'])
      for policy in policies:
        x,x2 = unzip(dict1[policy]['hits'])
        y1,y2 = unzip(dict1[policy]['misses'])
        vals = zip(x2,y2)
        y = map(lambda hm: hm[0]*1.0/((hm[0]+hm[1])*1.0), vals)
        plt.plot(x, y)
        plt.legend(policies, loc='upper right')
      output = Base + '/' + app_name + '/' + app_name + '_' + metric + '.png'
      output = directory + '/' + metric + '_' + predictorToTitle(predictor) + '_ ' + scheduler + '.png'
      plt.title(app_name + ' ' + predictorToTitle(predictor))
      plt.xlabel('Cache Size')
      plt.ylabel(metric)
      plt.savefig(output)
      plt.clf()

def memoryFile(dict,memory,jsfile,json_from_file,confs):
  app = dict.keys()[0]
  app_name = app.replace(" ", "")
  directory = Base + '/' + app_name + "/logs"
  simulations = json_from_file['simulations']
    
  if not os.path.exists(directory):
    os.makedirs(directory)
  sims = directory + '/simulations'
  if not os.path.exists(sims):
    os.makedirs(sims)
  simjson = {}
  for key in memory.keys():
    simjson[key] ={'simulation' : []}
  for sim in json_from_file['simulations']:
    id = sim['simulation id']
    simjson[id]['simulation'].append(sim)
  for key in memory.keys():
    conf = confs[str(key)]
    scheduler = conf['scheduler']
    predictor = conf['size predictor']
    policy = conf['policy']
    size = str(conf['memory capacity'])
    output = sims + '/' + str(key) + '_events_' + size + '_' + policy + '_' + predictor + '_' + scheduler
    outfile = open(output, 'w')
    print >> outfile, conf
    for e in memory[key]:
      print >> outfile, e
    output = sims + '/' + str(key) + '_jobs_' + size + '_' + policy + '_' + predictor + '_' + scheduler
    outfile = open(output, 'w')
    print >> outfile, conf
    print>>outfile, json.dumps(simjson[key], indent=4)

  copyfile(jsfile, directory + '/' + basename(jsfile))

fold = sys.argv[1]
Base = GraphsDir + '/' + fold
if not os.path.exists(Base):
  os.makedirs(Base) 

infile = sys.argv[2]
jsfile, memory = toJsonFile(infile)
json_from_file = open(jsfile)
inputDict = json.load(json_from_file)
dict,confs = toDict(inputDict)
print(dict)
toGraphs(dict)
toGraphs2(dict)
memoryFile(dict,memory,jsfile,inputDict,confs)
print(Base)

