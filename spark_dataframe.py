from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def load_movies_names():
    movies = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movies[ int(fields[0]) ] = fields[1]

    return movies

# for Hadoop
def load_movies_names_dataframe(line):
    fields = line.split('|')
    return Row(movie_id = int(fields[1]), rating = float(fields[2]))

if __name__=="__main__":
    spark = SparkSession.builder.appName("popular_movies").getOrCreate()

    movies_names = load_movies_names()

# load by spark
    lines = spark.sparkContext.textFile("hdfs://192.168.3.147:8999/home/maria_dev/igor/u.data")
# parse by spark to RDD
    movies_data_rdd = lines.map(load_movies_names_dataframe)
# get DataFrame  = dictionary
    movies_data_dataframe = spark.createDataFrame(movies_data_rdd)

#     calculation
# avg rating and count = simple functions
    avg_rating = movies_data_dataframe.groupBy("movie_id").avg("rating")
    movies_count = movies_data_dataframe.groupBy("movie_id").count()

#     join abg rating + movies_count + order by + limit 10
    avg_count = movies_count.join(avg_rating,"movie_id").orderBy("movie_id").take(10)

    for movie in avg_count:
        print ( movies_names[movie[0]], movie[1], movie[2])

    spark.stop()





