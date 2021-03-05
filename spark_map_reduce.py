from pyspark import SparkConf, SparkContext


def load_movies_names():
    movies = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movies[ int(fields[0]) ] = fields[1]

    return movies

# for Hadoop
def load_movies_names(line):
    fields = line.split('|')
    return (  int(fields[1]),  ( float(fields[2]), 1 )  )

# movie_id =1  rating =7.1  count_of_data 1
# movie_id =1  rating =2.1  count_of_data 1 =>    movie_id=1 sum_rating = 9.1 count_of_data=2

if __name__=="__main__":
    spark_conf = SparkConf().setAppName("popular_movies")
    connection = SparkContext(conf = spark_conf)


    movies_names = load_movies_names()

# load by spark
    lines = connection.textFile("hdfs://192.168.3.147:8999/home/maria_dev/igor/u.data")

    movies_rating = lines.map(load_movies_names)
                #( key=int(fields[1]), value= (float(fields[2]), 1) )
                # value 1 = (float(fields[2]), 1)
                # value 2 = (float(fields[2]), 1)
    rating = movies_rating.reduceByKey(lambda movie1, movie2 : (movie1[0]+ movie2[0],  movie1[1]+movie2[1])  )

    rating2 = movies_rating.filter(lambda column: column[1]>1000)

    #final operation
    # (float(fields[2]), 1)
    avg_rating = rating.mapValues(lambda value: value[0]/value[1])

    #sort
    sorder_avg = avg_rating.sortBy(lambda filter :filter[1])

    result = sorder_avg.filter(lambda column: column>10)#column = rating
    #result movie_id =1   rating = 45.33332
    for data in result:
        print (data[0], data[1])