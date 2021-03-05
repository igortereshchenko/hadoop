from pyspark import SparkConf, SparkContext

configuration = SparkConf().setAppName('Parallel')
sc = SparkContext(conf=configuration)

words = ["a", "b", "c", "d"]

# server 1 , server2 =>  ["a", "b"] =>server 1, ["c", "d"] =>server 2 => server 1.count +  server 2 count => total count

#
parallel_data = sc.parallelize(words)

# parallel count (see reduce)
element_count = parallel_data.count()

print(element_count)

# parallel collect data = UNION

all_data = parallel_data.collect()

print(all_data)


# apply parallel job to data
# for data processing
def job(data):
    print (data + '!')


parallel_data.foreach(job)


# parallel filter
def filter_implementation(data):
    if data == 'a':
        return True
    return False


filtered_data = parallel_data.filter(filter_implementation)

# VS

filtered_data = parallel_data.filter(lambda data: data == 'a')

filtered_data_collected = filtered_data.collect()

print (filtered_data_collected)

# map for data changing
new_parallel_data = parallel_data.map(lambda data: data + '!')
new_parallel_data_collected = new_parallel_data.collect()

print (new_parallel_data_collected)


# reduce set -> info about = SCALAR
def reducer(value1, value2):
    return value1 + value2


# [1,2,3] => reduce (1,2) = 3  => reduce(3,3)=> 6
adding = parallel_data.reduce(reducer)

print (adding)

# join
# data1 = (id, smth1)
# data2 = (id, smth2)
# data1.join(data2) = >    [   (id, (smth1, smth2) )       ]

# humans(id, name)
humans = [(1, 'Bob'), (2, 'Boba')]
# orders(order_id, human_id, date)
orders = [
    (1, 1, '10.10.2020'),
    (2, 1, '11.10.2020'),
    (3, 1, '12.10.2020'),
    (4, 2, '18.10.2020')
]
# create rdd
humans_parallel = sc.parallelize(humans)
orders_parallel = sc.parallelize(orders)

orders_parallel_ok = orders_parallel.map(
                                            lambda data:
                                                        ( data[1],  (data[0], data[2] )  )
                                        )

humans_join_orders = humans_parallel.join(orders_parallel_ok)

print (humans_join_orders.collect())

# Bobs orders
bobs_orders = humans_join_orders.filter(lambda data: data[1][0] == 'Bob')
print (bobs_orders.collect())

