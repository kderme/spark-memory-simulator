import sys
import os

spark_home = os.environ.get('SPARK_HOME')
file = sys.argv[1].rsplit('/', 1)[-1]
path = spark_home + '/dags_exported/' + file
open(path + '.clean','w').writelines([ line for line in open(path) if 'DAGINFO' in line])
os.remove(path)
