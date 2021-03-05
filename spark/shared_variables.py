from pyspark import SparkConf, SparkContext

configuration = SparkConf().setAppName('Parallel')
sc = SparkContext(conf=configuration)


# BROADCAST VARIABLE ~= reading

# words = sc.parallelize(["a", "b", "c", "d"]) distribuded by cluster
words = sc.broadcast(["a", "b", "c", "d"])
data = words.value

# index exists
element = words.value[1]

print (data)
print (element)

# ACCUMULATOR VARIABLE ~= writing => blocking

sum = sc.accumulator(0)

def summator(element):
    global sum
    sum+=element

number = sc.parallelize([1,2,3,4,5,6,7,8])
number.foreach(summator)

print (sum.value)
