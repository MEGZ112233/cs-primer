import unittest

from excecuter import MemoryScan, QueryBuilder, SortMergeJoin, Aggregator, run, HeapScan, Sort
from models import DataTypes, Schema, SortMergeActions
from utils import comperator, delete_files_after_test
from db_helper import seed_csv_into_heap


class TestAggregate(unittest.TestCase):
    def test_average_of_numbers(self):
        sequence_table = [[1], [2], [3], [4], [5]]
        schema = Schema(
            table_name="sequence", columns={"number": DataTypes.INT.value}
        )
        def key_func(row):
            return 0
        
        def agg_func(group):
            sum = 0
            for row in group:
                sum += row["sequence.number"]
            return sum / len(group)
        result = list(run(
            QueryBuilder(
                [Aggregator(key_func, agg_func), MemoryScan(schema=schema, table=sequence_table)],
                [-1, 0]
            )
        ))
        print(result)
        self.assertEqual(result, [3.0])

    def test_avreage_movies_ratings(self):
        try : 
            movies_schema = Schema(
                table_name="movies", columns={"movieId": DataTypes.INT.value, "title": DataTypes.STRING.value, "genres": DataTypes.STRING.value}
            )
            ratings_schema = Schema(
                table_name="ratings", columns={"userId": DataTypes.INT.value, "movieId": DataTypes.INT.value, "rating": DataTypes.FLOAT.value, "timestamp": DataTypes.INT.value}
            )
            seed_csv_into_heap("data/movies.csv", movies_schema, "movies.bin")
            seed_csv_into_heap("data/ratings.csv", ratings_schema, "ratings.bin",300000)
            def key_func(row):
                return row["movies.movieId"]
            
            def agg_func(group):
                sum = 0
                for row in group:
                    sum += row["ratings.rating"]
                return sum / len(group)
            
            result = list(run(
                QueryBuilder(
                    [
                    Aggregator(key_func, agg_func),
                    SortMergeJoin(comperator=comperator, left_key="movies.movieId", right_key="ratings.movieId"),
                    HeapScan(filepath="movies.bin", schema=movies_schema),
                    Sort(lambda x: x["ratings.movieId"]),
                    HeapScan(filepath="ratings.bin", schema=ratings_schema)],
                    [-1, 0, 1, 1, 3]
                )
            ))
        finally : 
            delete_files_after_test(["movies.bin", "ratings.bin"])
        self.assertGreater(len(result), 100)



if __name__ == "__main__":
    unittest.main()
