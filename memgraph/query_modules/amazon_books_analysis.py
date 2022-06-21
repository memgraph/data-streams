import mgp
from queue import PriorityQueue

'''
Sample trigger for calling write procedure: 

CREATE TRIGGER newBookRating
ON CREATE BEFORE COMMIT EXECUTE
UNWIND createdEdges AS e
CALL amazon_book_analysis.new_rating(e) YIELD *;
'''

@mgp.write_proc
def new_rating(
    context: mgp.ProcCtx,
    rating: mgp.Edge
) -> mgp.Record(Rating = mgp.Nullable[mgp.Edge],
                Book = mgp.Nullable[mgp.Vertex]):
    if rating.type.name == "RATED":           
        book = rating.to_vertex
        book_rating = rating.properties.get("rating")
        rating_sum = book.properties.get("rating_sum")
        if  rating_sum == None:
            book.properties.set("rating_sum", book_rating)
            book.properties.set("num_of_ratings", 1)
        else: 
            current_rating = rating_sum + book_rating
            book.properties.set("rating_sum", current_rating) 
            book.properties.set("num_of_ratings", book.properties.get("num_of_ratings") + 1)
        return mgp.Record(Rating=rating, Book=book)
    return mgp.Record(Rating=None, Book=None)

'''
Sample Query module call returns 10 books (if there are 10) with 60 or more ratings. 
CALL amazon_books_analysis.best_rated_books(10, 60)
YIELD best_rated_books
UNWIND best_rated_books AS Book
WITH Book[0] AS Rating, Book[1] as Title
RETURN Rating, Title
'''

@mgp.read_proc
def best_rated_books(
    context: mgp.ProcCtx,
    number_of_books: int,
    ratings_treshold: int
    
) -> mgp.Record(best_rated_books = list):

    q = PriorityQueue(maxsize=number_of_books)
    for book in context.graph.vertices:
        label, = book.labels
        if label.name == "Book": 
            num_of_ratings = book.properties.get("num_of_ratings")
            title = book.properties.get("title")
            if num_of_ratings != None and num_of_ratings >= ratings_treshold:
                rating = book.properties.get("rating_sum")/num_of_ratings
                if q.empty() or not q.full():
                    q.put((rating, title))
                else: 
                    top = q.get()
                    if top[0] > rating:
                        q.put(top)
                    else: 
                        q.put((rating, title))
                        

    books = list()
    while not q.empty():
        books.append(q.get())

    books.reverse()
    return mgp.Record(best_rated_books=books)

"""
MATCH (u:User{id:"A3NNFCL3ORBQUI"}) CALL amazon_books_analysis.recommend_books_for_user(u, 10, 60) 
YIELD recommended_books
UNWIND recommended_books AS Book
WITH Book[0] AS Rating, Book[1] as Title
RETURN Rating, Title
"""

    
@mgp.read_proc
def recommend_books_for_user(
    context: mgp.ProcCtx,
    user : mgp.Vertex,
    number_of_books: int,
    ratings_treshold: int
    
) -> mgp.Record(recommended_books = list):

    rated_books = []
    for user_ratings in user.out_edges:
        user_book = user_ratings.to_vertex
        rated_books.append(user_book.id)

    q = PriorityQueue(maxsize=number_of_books)
    for book in context.graph.vertices:
        label, = book.labels
        if label.name == "Book" and book.id not in rated_books: 
            num_of_ratings = book.properties.get("num_of_ratings")
            title = book.properties.get("title")
            if num_of_ratings != None and num_of_ratings >= ratings_treshold:
                rating = book.properties.get("rating_sum")/num_of_ratings
                if q.empty() or not q.full():
                    q.put((rating, title))
                else: 
                    top = q.get()
                    if top[0] > rating:
                        q.put(top)
                    else: 
                        q.put((rating, title))
                        

    books = list()
    while not q.empty():
        books.append(q.get())

    books.reverse()
    return mgp.Record(recommended_books=books)           
