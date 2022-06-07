"""
Sample trigger for calling write procedure: 

CREATE TRIGGER newMovieRating
ON CREATE AFTER COMMIT EXECUTE
UNWIND createdEdges AS e
CALL movie_analysis.new_rating(e) YIELD *;
"""
import mgp
from queue import PriorityQueue

@mgp.write_proc
def new_rating(
    context: mgp.ProcCtx,
    rating: mgp.Edge
) -> mgp.Record(Rating = mgp.Nullable[mgp.Edge],
                Movie = mgp.Nullable[mgp.Vertex]):
    if rating.type.name == "RATED":           
        movie = rating.to_vertex
        movie_rating = rating.properties.get("rating")
        rating_sum = movie.properties.get("rating_sum")
        if  rating_sum == None:
            movie.properties.set("rating_sum", movie_rating)
            movie.properties.set("num_of_ratings", 1)
        else: 
            current_rating = rating_sum + movie_rating
            movie.properties.set("rating_sum", current_rating) 
            movie.properties.set("num_of_ratings", movie.properties.get("num_of_ratings") + 1)
        return mgp.Record(Rating=rating, Movie=movie)
    return mgp.Record(Rating=None, Movie=None)


"""
Sample query module call that returns 10 movies (if there are 10) that have 4 or more ratings. 
CALL movie_analysis.best_rated_movies(10, 4) YIELD *
"""

@mgp.read_proc
def best_rated_movies(
    context: mgp.ProcCtx,
    number_of_movies: int,
    ratings_treshold: int
) -> mgp.Record(best_rated_movies = list):

    q = PriorityQueue(maxsize=number_of_movies)
    for movie in context.graph.vertices:
        label, = movie.labels
        if label.name == "Movie": 
            num_of_ratings = movie.properties.get("num_of_ratings")
            title = movie.properties.get("title")
            if num_of_ratings >= ratings_treshold:
                rating = movie.properties.get("rating_sum")/num_of_ratings
                if q.empty():
                    q.put((rating, title))
                else: 
                    if not q.full():
                        q.put((rating, title))
                    else: 
                        top = q.get()
                        if top[0] > rating:
                            q.put(top)
                        else: 
                            q.put((rating, title))
                        
    movies = list()
    while not q.empty():
        movies.append(q.get())

    movies.reverse()
    return mgp.Record(best_rated_movies=movies)
