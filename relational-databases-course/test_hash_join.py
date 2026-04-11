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
    def test_triple_join(self):
        "join three tables on movieId and assert the result is correct"
        seq_table = [
            [1],[2],[3]
        ]
        seq_schema1 = Schema(
            table_name="seq1",
            columns = {
            'number': 'int32'
            }
        )

        seq_schema2 = Schema(
            table_name="seq2",
            columns = {
            'number': 'int32'
            }
        )
        seq_schema3 = Schema(
            table_name="seq3",
            columns = {
            'number': 'int32'
            }
        )
        get_iterable_table_key1  = lambda row : 1
        get_hash_table_key1 = lambda row : 1

        HashJoin(
            get_iterable_table_key1,
            get_hash_table_key1
        )

        get_iterable_table_key2  = lambda row :1
        get_hash_table_key2 = lambda row : 1
        

        result = list(run(
            QueryBuilder(
                [
                    HashJoin(get_hash_table_key2,get_iterable_table_key2),
                    MemoryScan(seq_schema3, seq_table),
                    HashJoin(get_hash_table_key1,get_iterable_table_key1),
                    MemoryScan(seq_schema1, seq_table),
                    MemoryScan(seq_schema2, seq_table)
                ],
                [-1,0,0,2,2]
            )
        )
        )
        print(result)
        self.assertEqual(len(result), 27, f"the result have this wrong lenth {len(result)}")
        ## TODO : make the builing of the query plan more intiutive and less error prone

if  __name__ == "__main__" : 
    unittest.main()
        