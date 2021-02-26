from mrjob.job import MRJob
from mrjob.step import MRStep


class avarage_rating(MRJob):

    def steps(self):
        return [

            MRStep(mapper = self.mapper_movie_watch, reducer = self.reducer_watch_counter),
            MRStep(reducer=self.reducer_final_step)

        ]



    def mapper_movie_watch(self,_,line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, int(rating)


    def reducer_watch_counter(self,key,values):
        sum=0
        count=0
        for rating in values:
            sum+=rating
            count+=1


        yield  key, {'sum':sum,'count':count}






    def reducer_final_step(self, key, values):
        data = next(values)

        if data['count']:
            yield key, data['sum']/data['count']
        else:
            yield key, 0







if __name__=='__main__':
    avarage_rating.run()


