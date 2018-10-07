#!/usr/bin/python

import sys
import os
import json

import plotly.plotly as py
import plotly.graph_objs as go
import matplotlib.pyplot as plt
import ntpath

def keep (line):
  flag1 = '|| SIMULATION ||' in line
  flag2 = '&&' not in line
  flag3 = '|| SIMULATION || Predicting..' not in line
  return flag1 and flag2 and flag3

def find_last_job(data):
  job = -1
  for line in data:
    if '|| SIMULATION ||   jobid = ' in line:
      job = int(line.split('=')[1])
  return job
 
def toJson (file):
  data = [line.strip() for line in open(file, 'r')]
  newdata = []
  for line in data:
    if keep(line):
      print line
      line1 = line.split('||')[2]      
      if line1.startswith(' }'):
        newdata.append('  "valid" : true')
        line1 = ' },'
      if 'No results. Simulation failed' in line1:
        newdata.append(' "valid" : false')
        line1 = ' },'
      if  '=' in line1:
        line1 = '  "' + line1.split('=')[0].strip()+ '" : "' + line1.split('=')[1].strip() +'",'
      newdata.append(line1)
  print newdata
  outfile = open(file + '1.json', 'w')
  print>>outfile, '['
  for line in newdata[:-1]:
    print>>outfile, line
  print>>outfile, '}]'
  return file + '1.json'

spark_home = os.environ.get('SPARK_HOME')
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

dict = {}
dict[file]={}
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


