import unittest 
import os

from excecuter import * 
from config import DATA_DIR
from utils import seed_csv_into_heap, delete_files_after_test


schema1 = Schema(
                table_name="movies1",
                columns = {
                'movieId' : 'int32',
                'title' : 'text',
                'genres' : 'text'
            }
            )
schema2 = Schema(
    table_name="movies2",
    columns = {
    'movieId' : 'int32',
    'title' : 'text',
    'genres' : 'text'
}
)
heap_filename_1 = "movies1.bin"
heap_filename_2 = "movies2.bin"

def setUpModule():
    
    

    table_file_path = os.path.join(DATA_DIR, "movies.csv")
    seed_csv_into_heap(table_file_path, schema1, heap_filename_1)
    seed_csv_into_heap(table_file_path, schema2, heap_filename_2)


def tearDownModule():
    delete_files_after_test([heap_filename_1, heap_filename_2])

           


class TestNestedLoops(unittest.TestCase):
                
     def test_basic_nested_loop(self):
         "read 10 row from each table and nest loop them and then assert they are 100"
        #  result = list(run(
        #     QueryBuilder(
        #         [   
        #             NestedLoopJoin(),
        #             Limit(10),
        #             HeapScan(heap_filename_2,schema2),                   
        #             Limit(10),
        #             HeapScan(heap_filename_1,schema1)
        #         ],
        #         [
        #          -1, 0, 1, 0, 3
        #         ]
        #     )    
        #  )
        #  )
        #  self.assertEqual(len(result),100)
         pass
           

     
     def test_selectiion_after_nested_loops(self):
        #  result = list(run(
        #     QueryBuilder(
        #         [   
        #             NestedLoopJoin(),
        #             Selection(lambda x: x['movies2.movieId'] < 6),
        #             HeapScan(heap_filename_2,schema2),                   
        #             Selection(lambda x: x['movies1.movieId'] < 5),
        #             HeapScan(heap_filename_1,schema1)
        #         ],
        #         [
        #          -1, 0, 1, 0, 3
        #         ]
        #     )    
        #  )
        #  )
        #  self.assertEqual(len(result),20)
        pass
     
     def test_selection_after_nested_loops(self):
            result = list(run(
                QueryBuilder(
                    [   Selection(lambda x: x['movies2.movieId'] < 6 and x['movies1.movieId'] < 6),
                        NestedLoopJoin(),
                        Limit(10),
                        HeapScan(heap_filename_2,schema2),  
                        Limit(10),                 
                        HeapScan(heap_filename_1,schema1)
                    ],
                    [
                    -1, 0, 1, 2, 1, 4 
                    ]
                )    
            )
            )   
            self.assertEqual(len(result),25)
     
     def test_self_join(self):
         result = list(run(
            QueryBuilder(
                [   
                    NestedLoopJoin(),
                    Limit(10),
                    HeapScan(heap_filename_1,schema1),                   
                    Limit(10),
                    HeapScan(heap_filename_1,schema1)
                ],
                [
                 -1, 0, 1, 0, 3
                ]
            )    
         )
         )
         self.assertEqual(len(result),100)
     
if __name__ == '__main__':
    unittest.main()