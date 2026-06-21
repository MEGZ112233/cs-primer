from excecuter import CSVScan, Insert, QueryBuilder, run, Limit 
from models import Schema

def seed_csv_into_heap(csv_file_path, schema:Schema, outpute_file_name, limit = 10000000000): 
    if outpute_file_name is None : 
       outpute_file_name  = f"{schema.table_name}.bin"
    result_of_insert =list(run(
        QueryBuilder(
            [
                Insert(outpute_file_name, schema),
                Limit(limit),
                CSVScan(csv_file_path,schema)
            ], 
            [
            -1,
            0,
            1
            ]
        )
    )
    )
    print(f"result of insert is {len(result_of_insert)}")