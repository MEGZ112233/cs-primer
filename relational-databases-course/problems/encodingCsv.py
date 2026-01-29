import csv

import string
from typing import Callable
from pathlib import Path
PAGE_SIZE = 1 << 12  # 4KB
### i need to make a factory for encoding a value 
class Encoder : 
    
def encode_value(value):
    """
    this we will pass to it a column and it will convert to it a string and then we will encode it using this 
    encoded_in_binary =. len(value) + encoded_value 
    """
    value = str(value)
    ret = len(value).to_bytes(4) + value.encode('utf-8')
    return ret 

def decode_value(bin_file , column_type : Callable) : 
    """
    this we will pass to it a binary encoded value and it will decode it to original value .
    """
    try : 
        number_of_bytes = int.from_bytes(bin_file.read(4),'big')
        print(f'the number of bytes is {number_of_bytes}')
        column_value = column_type(bin_file.read(number_of_bytes).decode('utf-8'))
        print(f'column_info ({number_of_bytes} , {column_value})')
        return column_value
    except Exception as e  : 
        raise Exception(f'Error {e} while decoding a value') 

def encode_row(row): 
    encoded_row = None
    for column in row : 
        encoded_column = encode_value(column)
        if encoded_row is None : 
           encoded_row = encoded_column
        else : 
            encoded_row += encoded_column
    return encoded_row

def decode_row(bin_file , schema) : 
    """
    this we will pass to it a binary file and a schema and it will decode it to original row .
    """
    row = list()
    for key , type in schema.items(): 
        try : 
            column = decode_value(bin_file , type)
        except Exception as e : 
            raise e
        row.append(column)
    
    return row
### we can make a class called Page     
class Page : 
    def __init__(self , file_path , start_pointer) : 
        self.file_path = file_path 
        self.start_pointer = start_pointer
        self.data = bytearray()
        self.number_of_rows = 0
        self.update_page_header()
        self.is_flushed = False
    def update_page_header(self) : 
        self.data[0:2] = self.number_of_rows.to_bytes(2)

    def add_row(self , row) : 
        encoded_row = encode_row(row)
        if len(encoded_row) + len(self.data) > PAGE_SIZE  :
           return False  
        self.data += encoded_row
        self.number_of_rows += 1
        self.update_page_header()
        return True

    def flush_on_disk(self) :
        Path(self.file_path).touch(exist_ok=True)
        with open(self.file_path , 'r+b') as f : 
            f.seek(self.start_pointer)
            f.write(self.data)
        self.is_flushed = True




         

def convert_csv_file_to_binary_format(file_path :string): 
    """
    this function will do that following steps : 
    1 - read the csv file from the given file . 
    2 - the row will be converted to a binary format using the encode_row function
    3 - write the encoded row t a binary file with the same name but just with .bin extenstion
    4 - return the path of the binary file created . 
    """
    number_of_pages  = 0 
    bytes_used = 0
    bin_path = file_path.replace('csv' , 'bin')
    page = Page(bin_path, bytes_used)
    with open(file_path, 'r') as f: 
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        for row in csv_reader : 
            if page.add_row(row) : 
                continue 
            page.flush_on_disk()
            number_of_pages += 1
            bytes_used += PAGE_SIZE
            page = Page(bin_path , bytes_used)

        if page.number_of_rows > 0 and page.is_flushed is False  : 
           page.flush_on_disk()

            
                     

def read_db_file(file_path , schema,  number_of_rows) : 
    """
    we will read number of rows from the binary file and return them as a list of decoded rows
    """
    db_result = []
    current_cursor = 0
    ### 1 - read at first the number of rows in page 
    ### 2 - row by row 
    with open(file_path ,'rb' ) as bin_file : 
        try :
            while number_of_rows > 0 : 
                bin_file.seek(current_cursor)
                number_of_rows_in_page = int.from_bytes(bin_file.read(2) , 'big')
                decoded_row = decode_row(bin_file , schema)
                db_result.append(decoded_row)
                current_cursor += PAGE_SIZE 
        except  Exception as e  : 
                print(e)
        finally : 
            bin_file.close()
            
    return db_result



if __name__ == '__main__' : 
    # convert_csv_file_to_binary_format('orders.csv')
    # schema = {
    #     'id_order' : int, 
    #     'order_code' : str,
    #     'src_address' : str,
    #     'dst_address' : str,
    #     'total_cost' : float
    # }
    # res = read_db_file('orders.bin' , schema , 6)

    # print(1<<12)
    pass 