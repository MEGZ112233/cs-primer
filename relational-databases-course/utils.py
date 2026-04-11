from excecuter import * 

def seed_csv_into_heap(csv_file_path, schema:Schema, outpute_file_name): 
    if outpute_file_name is None : 
       outpute_file_name  = f"{schema.table_name}.bin"
    result_of_insert =list(run(
        QueryBuilder(
            [
                Insert(outpute_file_name, schema),
                CSVScan(csv_file_path,schema)
            ], 
            [
            -1,
            0 
            ]
        )
    )
    )
    print(f"result of insert is {len(result_of_insert)}")
    


def delete_files_after_test(file_names:list):
    for file_path in file_names : 
        if os.path.exists(file_path) :
           os.remove(file_path) 

def format_row(row, schema:Schema):
    # TODO :  make and scan Node to use this function 
    formatted_row = {}
    index = 0 
    for column_name, type in schema.columns.items() : 
        formatted_row[f"{schema.table_name}.{column_name}"] = row[index]
        index += 1 

    return formatted_row


