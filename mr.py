from mrjob.job import MRJob
from mrjob.step import MRStep


class mr(MRJob):

    def steps(self):
        return [

            MRStep(mapper = self.mapper_movie_watch, reducer = self.reducer_watch_counter),
            MRStep(reducer=self.reducer_final_step)

        ]


# movie_id  rating    =>  movie_id  is_watch
# 1           3             1         1
# 2           5             2         1
# 2           1             2         1
# 3           5             3         1

    def mapper_movie_watch(self,_,line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1

#  data for reducer by Hadoop
# movie_id  is_watch
# 1: {1}
# 2: {1,1}
# 3: {1}


    def reducer_watch_counter(self,key,values):
        yield  str(sum(values)).zfill(7), key


#  data for next step reducer by Hadoop
# count movie_id
# 000001: {1, 3}
# 000002: {2}

    def reducer_final_step(self, key, values):
        for movie in values:
            yield movie, key

# 1: {000001}
# 3: {000001}
# 2: {000002}







if __name__=='__main__':
    mr.run()


