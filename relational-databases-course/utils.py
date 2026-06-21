from models import Schema, SortMergeActions
import os

def comperator(left_key, right_key, left_row, right_row):
    left_value = left_row[left_key] if left_row else None
    right_value = right_row[right_key] if right_row else None
    if left_value is None:
        return SortMergeActions.GET_LEFT_NO_MATCH
    elif right_value is None:
        return SortMergeActions.GET_RIGHT_NO_MATCH
    elif left_value > right_value:
        return SortMergeActions.GET_RIGHT_NO_MATCH
    elif left_value < right_value:
        return SortMergeActions.GET_LEFT_NO_MATCH
    elif left_value == right_value:
        return SortMergeActions.GET_RIGHT_MATCH
    


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


