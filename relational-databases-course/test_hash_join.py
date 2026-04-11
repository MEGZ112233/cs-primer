import unittest 
from excecuter import MemoryScan, Schema, HashJoin, QueryBuilder, run

rating_schema = Schema(
    table_name="ratings",
    columns = {
    'userId': 'int32',
    'id_movies': 'int32',
    'rating': 'float',
    'timestamp': 'int32'
    }
)

movie_schema = Schema(
    table_name="movies",
    columns = {
    'movieId': 'int32',
    'title': 'text',
    'genres': 'text'
    }
)
rating_memory_table = [
    [1, 1, 4.0, 964982703],
    [1, 1, 4.0, 964981247],
    [1, 2, 4.0, 964982224],
    [1, 3, 5.0, 964982931],
    [1, 4, 5.0, 964983815],
    [1, 5, 5.0, 964983815],
    [1, 5, 5.0, 964983817],
    [1, 5, 5.0, 964983815]
]

movie_memory_table = [
    [1, 'Toy Story (1995)', 'Adventure|Animation|Children|Comedy|Fantasy'],
    [2, 'Jumanji (1995)', 'Adventure|Children|Fantasy'],
    [3, 'Grumpies (1995)', 'Comedy|Fantasy'],
    [4, 'Waiting to Exhale (1995)', 'Comedy|Drama|Romance']
]

class TestHashJoin(unittest.TestCase):
    def test_basic_hash_join(self):
        "join two tables on movieId and assert the result is correct"
        get_iterable_table_key  = lambda row : row['ratings.id_movies']
        get_hash_table_key = lambda row : row['movies.movieId']
        result = list(run(
            QueryBuilder(
                [   
                    HashJoin(get_hash_table_key,get_iterable_table_key),
                    MemoryScan(movie_schema, movie_memory_table),                   
                    MemoryScan(rating_schema, rating_memory_table)
                ],
                [
                 -1, 0, 0
                ]
            )    
        ))
        self.assertEqual(len(result), 5, f"the result have this wrong lenth {len(result)}")

if  __name__ == "__main__" : 
    unittest.main()
        