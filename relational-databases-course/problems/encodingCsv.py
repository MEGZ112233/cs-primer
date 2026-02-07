import csv

import string
from typing import Callable
from pathlib import Path
import io 
PAGE_SIZE = 1 << 12  # 4KB
### i need to make a factory for encoding a value 
class Encoder : 
        
    def encode_int32(number):
        number = int(number) 
        encoded_value = number.to_bytes(4)
        return encoded_value


    def encode_text(text) :
        text = str(text)
        encoded_value = text.encode('utf-8')
        return encoded_value
    
    @classmethod
    def encode_value(cls , value , data_type):
        """
        this we will pass to it a value and  then we will encode based on the datatype 
        """
        try  : 
            method_name = f'encode_{data_type}'
            method = getattr(cls , method_name)
            encoded_value = method(value)
            print(encoded_value)
            return encoded_value
        except AttributeError  as e  :  
            raise Exception(f'this {data_type} is not supported to be encoded')
class Decoder : 
        def decode_int32(number): 
            encoded_value = int.from_bytes(number, 'big')
            return encoded_value


        def decode_text(text) :
            text = str(text)
            encoded_value = text.encode('utf-8')
            return encoded_value
        
        @classmethod
        def decode_value(cls , value , data_type):
            """
            this we will pass to it a value and  then we will encode based on the datatype 
            """
            try  : 
                method_name = f'encode_{data_type}'
                method = getattr(cls , method_name)
                encoded_value = method(value)
                print(encoded_value)
                return encoded_value
            except AttributeError  as e  :  
                raise Exception(f'this {data_type} is not supported to be encoded')
            
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

def encode_row(row , schema): 
    encoded_row = io.BytesIO()
    for column , datatype in zip(row , schema) : 
        encoded_column = Encoder.encode_value(column , datatype)
        encoded_row.write(encoded_column)
    
    return encoded_row.read()
def decode_page(bin_file , page_number , schema) : 
    """
    this we will pass to it a binary file and a schema and it will decode it to original row .
    """
    row = list()
    bin_file.seek(page_number*PAGE_SIZE)
    page_data = bin_file.read(PAGE_SIZE)
    number_of_rows = int.from_bytes(page_data[0:2])
    range_end = PAGE_SIZE
    if number_of_rows == 0  : 
       return None
    
    for i in range(1 , number_of_rows+1) :  
        try : 
            range_start = int.from_bytes(page_data[i*2:(i+1)*2])
            row_data = page_data[range_start:range_end]
            range_end = range_start
            decode_row()
        except Exception as e : 
            raise e
        row.append(column)
    
    return row
### we can make a class called Page     
class Page : 
    ### the slotted_page will contain three main things the first the first two bytes and then we will add from then a refernce will take also  where this record finish 
    ## we will and the
    def __init__(self , file_path , file_pointer , schema) : 
        self.file_path = file_path 
        self.file_pointer = file_pointer
        self.data = bytearray(PAGE_SIZE)
        self.number_of_rows = 0
        self.schema = schema 
        self.update_page_header()
        self.is_flushed = False
    def update_page_header(self) : 
        self.data[0:2] = self.number_of_rows.to_bytes(2)

    def get_last_index_used(self) : 
        reference_index = (self.number_of_rows+1)*2
        last_index_used = int.from_bytes(self.data[reference_index:reference_index+2])
        if last_index_used == 0 : 
           last_index_used = PAGE_SIZE  
        return last_index_used
    
    def add_row(self , row) : 
        encoded_row = encode_row(row , self.schema)
        ## we will add the encoded row to the page data 
        ## we will the offest of the roww the offset means where the starts 
        ## the index of the start f
        
        row_end_range = self.get_last_index_used()
        row_start_range = row_end_range - len(encoded_row)
        if row_start_range < (self.number_of_rows+2)*2 : 
            return False
        self.data[row_start_range:row_end_range] = encoded_row     
        self.number_of_rows += 1
        self.update_page_header()
        return True

    def flush_on_disk(self) :
        Path(self.file_path).touch(exist_ok=True)
        with open(self.file_path , 'r+b') as f : 
            f.seek(self.file_pointer)
            f.write(self.data)
        self.is_flushed = True




         

def convert_csv_file_to_binary_format(file_path :string , schema): 
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
    page = Page(bin_path, bytes_used , schema)
    with open(file_path, 'r') as f: 
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        for row in csv_reader : 
            if page.add_row(row): 
               continue 
            page.flush_on_disk()
            number_of_pages += 1
            bytes_used += PAGE_SIZE
            page = Page(bin_path , bytes_used , schema)

        if page.number_of_rows > 0 and page.is_flushed is False  : 
           page.flush_on_disk()

            
                     

def read_db_file(file_path , schema) : 
    """
    we will read number of rows from the binary file and return them as a list of decoded rows
    """
    db_result = []
    current_cursor = 0
    ### 1 - read at first the number of rows in page 
    ### 2 - row by row 
    with open(file_path ,'rb' ) as bin_file : 
        try :
            page_number = 0 
            while decode_page(bin_file ,page_number , schema) : 
                  page_number += 1 
        except  Exception as e  : 
                print(e)
        finally : 
            bin_file.close()
            
    return db_result



if __name__ == '__main__' : 
    convert_csv_file_to_binary_format('orders.csv' , ['int32' , 'text' , 'text' , 'text'])
    read_db_file('orders.bin' , ['int32' , 'text' , 'text' , 'text'])
    pass 
