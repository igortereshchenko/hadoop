from pyspark import SparkConf, SparkContext

# configuration = SparkConf().setAppName('Parallel')
# sc = SparkContext(conf=configuration)

# VS

from pyspark.sql import SparkSession
from pyspark.sql import Row

sc = SparkSession.builder.appName('Parallel').getOrCreate().sparkContext

df = sc.parallelize([

    (1, 2, 3, 'a'),
    (4, 5, 6, 'b'),
    (7, 8, 9, 'c')
]).toDF(['col1', 'col2', 'col3', 'col4'])

df.show()

session = SparkSession.builder.appName('Parallel').getOrCreate()

human_df = session.createDataFrame(data=[
    ('1', 'Bob', 17),
    ('2', 'Boba', 18),
    ('3', 'Boban', 19)
],
    schema=['id', 'name', 'age']
)

human_df.show()

human_csv_data = session.read.format('csv').option('header', 'true').load('/user/maria_dev/data.csv')
# human_csv_data = session.read.csv('/home/maria_dev/igor/spark/data.csv')

human_csv_data.show()
human_csv_data.printSchema()

# RDD -> DataFrame
words = [
    {"id": 1, "name": 'Bob', "age": 17},
    {"id": 2, "name": 'Boba', "age": 18},

]
parallel_data = sc.parallelize(words)

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

schema = StructType([

    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),

])

df = session.createDataFrame(data=parallel_data, schema=schema)
df.show()
df.printSchema()

# nested dataframes

data = [
    (('Bob', 'Kyiv'), 'NN1234', 'M', ('Zaz', 1234)),
    (('Boba', 'Lviv'), 'FG1234', 'F', ('Infinity', 5678))
]

schema = StructType(
    [
        StructField('human', StructType([
            StructField('name', StringType(), True),
            StructField('city', StringType(), True),
        ]), True),

        StructField('passport', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('car', StructType([
            StructField('car_model', StringType(), True),
            StructField('car_number', IntegerType(), True),
        ]), True)
    ]
)

df = session.createDataFrame(data=data, schema=schema)
df.show()

# queries
# select
df.select('passport').show()

# multiple files
order_df = session.read.options(header='true').csv('/user/maria_dev/order.csv', encoding='utf-8')
user_df = session.read.options(header='true').csv('/user/maria_dev/data.csv', encoding='utf-8')

join_df = user_df.join(order_df, user_df.id == order_df.fk_id).filter(user_df["name"]=="Bob")
join_df.show()


join_df.write.option("header",'true').csv('/user/maria_dev/join.csv')

print (join_df.count)
