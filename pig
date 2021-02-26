ratings = LOAD '/user/maria_dev/ml_movies/u.data' as (user_id: int, movie_id:int, rating:int, timestamp:int);

items = LOAD '/user/maria_dev/ml_movies/u.item' USING PigStorage('|')
	as (movie_id:int, movie_name:chararray, release_date:chararray, something:chararray, link:chararray);

items_new = FOREACH items GENERATE movie_id,  movie_name , ToUnixTime(ToDate(release_date, 'dd-MMM-yyyy')) as release_time;


ratings_by_movie = GROUP ratings By movie_id;

avg_ratings = FOREACH ratings_by_movie GENERATE group as movie_id, AVG(ratings.rating) as avg_rating;


DESCRIBE items;
DESCRIBE ratings_by_movie;
DESCRIBE avg_ratings;

best_movies = FILTER avg_ratings BY avg_rating>4.5;

best_movies_info = JOIN best_movies BY movie_id, items_new BY movie_id;

DESCRIBE best_movies_info;


order_best_movies_info = ORDER best_movies_info BY items_new::release_time;

DUMP best_movies_info;
