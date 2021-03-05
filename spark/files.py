from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles

from pyspark import MarshalSerializer

configuration = SparkConf().setAppName('Parallel')
# sc = SparkContext(conf=configuration)
sc = SparkContext('local',appName='Parallel', serializer=MarshalSerializer())


path = '/home/maria_dev/igor/spark/rdd.py'
filename = 'rdd.py'

sc.addFile(path)

print (SparkFiles.getRootDirectory())
print (SparkFiles.get(filename))

# take(n) ~= LIMIT 10
print(
    sc.parallelize(
                    list(range(10000))
                ).map(lambda element: element*element).take(10)
    )

sc.stop()
