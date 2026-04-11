import csv
import os 
from dataclasses import dataclass
from models import Schema
import encodingCsv
from encodingCsv import PAGE_SIZE
from pathlib import Path
from collections import defaultdict
from utils import format_row
class CSVScan(object):
    """
    Yield all records from the given "table" in memory.

    This is really just for testing... in the future our scan nodes
    will read from disk.
    """
    def __init__(self, filepath , schema:Schema):
        self.filepath = filepath
        self.offset = 0
        self.schema = schema
        with open(self.filepath, 'rb') as f:
            f.seek(0)
            header_line = f.readline()
            self.header = csv.reader([header_line.decode('utf-8')]).__next__()
            self.offset = f.tell()
        assert set(self.schema.columns.keys()) == set(self.header) , 'column names in the schema should be the same as column names in the csv file'

    def cast_variable(self,type_str, value) :
        if type_str == 'int32'  : 
           return int(value)  
        elif type_str == 'text' : 
            return str(value) 
        else :
            raise ValueError(f'type : {type_str} is not supported in our DB')
        
    def next(self):
        with open(self.filepath , 'rb') as f:
            try : 
                f.seek(self.offset)
                row = f.readline()
                row = csv.reader([row.decode('utf-8')]).__next__()
                if not row:
                    return None
                self.offset = f.tell()
                final_row = {}
                if  self.schema :
                    for column_name, column_type in self.schema.columns.items():
                        value_index = self.header.index(column_name)
                        final_row[f'{self.schema.table_name}.{column_name}'] = self.cast_variable(column_type,row[value_index])
                return final_row
            
            except Exception as e:
                print("Error reading file:", e)
                return None

#  1 - make the formate a dictionary (column names as keys) and values . 


class HeapScan(object):
    def __init__(self , filepath, schema: Schema ) : 
        self.filepath = filepath
        self.schema = schema 
        self.reset()

    def read_page(self) : 
        page_start_pointer = self.number_of_consumed_pages * encodingCsv.PAGE_SIZE
        with open(self.filepath , 'rb') as f : 
            f.seek(page_start_pointer)
            self.page_data = f.read(encodingCsv.PAGE_SIZE)
        self.number_of_consumed_pages += 1
        self.row_index = 0 
        return 

    def get_row_index(self) : 
        start_range = int.from_bytes(self.page_data[(self.row_index+1)*2: (self.row_index+2)*2])
        end_range = encodingCsv.PAGE_SIZE
        if self.row_index != 0 : 
            end_range = int.from_bytes(self.page_data[self.row_index*2:(self.row_index+1)*2]) 
        return start_range , end_range

    def decode_row(self , start_range , end_range) : 
        start_index = 0 
        row_data = self.page_data[start_range : end_range]
        row = {}
        for column_name, datatype in self.schema.columns.items(): 
            value = None
            if datatype == 'int32' : 
                value = int.from_bytes(row_data[start_index:start_index+4] , 'big')
                start_index+=4
            elif datatype == 'text' : 
                len_of_text = int.from_bytes(row_data[start_index:start_index+2]) 
                start_index+=2 
                value = row_data[start_index:start_index+len_of_text].decode('utf-8')
                start_index+=len_of_text
            
            column_index = f"{self.schema.table_name}.{column_name}"
            row[column_index] = value
            
        return row 
    
    def next(self) : 
        """
        get next row 
        """
        number_of_rows = int.from_bytes(self.page_data[0:2] , 'big')
        if number_of_rows == 0 : 
            return None 
        start_range , end_range = self.get_row_index()
        row = self.decode_row(start_range , end_range)
        self.row_index += 1
        if number_of_rows == self.row_index : 
           self.read_page()
        return row 
        ## handle when the we at the last index in page

    def reset(self) :
        self.number_of_consumed_pages = 0 
        self.page_data:bytes = None
        self.read_page()
        self.row_index =  0  # zero indexed    
class readWholeCSVFile(object):
    def __init__(self, filepath , schema = None):
        self.filepath = filepath
        self.schema = schema
        self.table = []
        self.index = 0 
        with open(self.filepath , 'r' , newline='' , encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if self.schema:
                    for i in range(len(self.schema)):
                        _, field_type = self.schema[i]
                        row[i] = field_type(row[i])
                self.table.append(tuple(row))

    def next(self) :
        if self.index >= len(self.table):
            return None
        x = self.table[self.index]
        self.index += 1
        return x
    
class MemoryScan(object):
    """
    Yield all records from the given "table" in memory.

    This is really just for testing... in the future our scan nodes
    will read from disk.
    """
    def __init__(self, schema:Schema,  table):
        self.table = table
        self.schema = schema
        self.reset()

    def next(self):
        if self.idx >= len(self.table):
            return None

        row = self.table[self.idx]
        formatted_row = format_row(row, self.schema)
        self.idx += 1
        return formatted_row
    
    def reset(self) :
        self.idx = 0
class GroupBy(object):
    """
    Group the child records using the given key function, and aggregate
    using the given aggregate function.

    Note that this implementation reads all child records into memory
    before yielding any output.
    """
    def __init__(self, key_func, agg_func):
        self.key_func = key_func
        self.agg_func = agg_func
        self.groups = None
        self.is_computed = False
        self.group_keys = None
        self.idx = 0
    
    def group_records(self) :
        self.groups = {}
        while True:
            x = self.childs[0].next()
            if x is None:
                break
            key = self.key_func(x)
            if key not in self.groups:
                self.groups[key] = []
            self.groups[key].append(x) 
        self.group_keys = list(self.groups.keys())
        self.idx = 0
    
    def next(self):
        if not self.is_computed:
            self.group_records()
        if self.idx >= len(self.group_keys):
            return None

        key = self.group_keys[self.idx]
        records = self.groups[key]
        self.idx += 1
        row = self.agg_func(key, records)
        return row
    
    def reset(self) :
        self.idx = 0

class Projection(object):
    """
    Map the child records using the given map function, e.g. to return a subset
    of the fields.
    """
    def __init__(self, proj):
        self.proj = proj

    def next(self):
        x = self.childs[0].next()
        if x is None:
            return None
        return self.proj(x)


class Selection(object):
    """
    Filter the child records using the given predicate function.

    Yes it's confusing to call this "selection" as it's unrelated to SELECT in
    SQL, and is more like the WHERE clause. We keep the naming to be consistent
    with the literature.
    """
    def __init__(self, predicate):
        self.predicate = predicate


    def next(self):
        while True:
            x = self.childs[0].next()
            if x is None or self.predicate(x):
                return x
    
    def reset(self) :
        self.childs[0].reset()
            
class Limit(object):
    """
    Return only as many as the limit, then stop
    """
    def __init__(self, limit ,  offset=0):
        self.original_limit = limit
        self.limit = limit
        self.offset = offset

    def next(self):
        x = self.childs[0].next()
        if x is None or self.limit  <= 0:
            return None
        if self.offset  > 0 :
            self.offset  -= 1
            return self.next()
        self.limit  -= 1
        return x
    
    def reset(self):
        self.childs[0].reset() 
        self.limit = self.original_limit

class Sort(object):
    """
    Sort based on the given key function
    """
    def __init__(self, key, desc=False):
        self.key = key
        self.desc = desc
        self.arr = None
        self.idx = 0
        self.sorted = False
    def compute(self):
        
        while True:
                x = self.childs[0].next()
                if x is None:
                    break
                if self.arr is None:
                    self.arr = []
                self.arr.append(x)
        self.arr = sorted(self.arr, key=self.key, reverse=self.desc)
        self.sorted = True 

    def next(self):
        if not self.sorted:
           self.compute 
        if self.arr is None or self.idx >= len(self.arr):
            return None
        x = self.arr[self.idx]
        self.idx += 1
        return x
    def reset(self) : 
        self.index = 0 

class Insert(object) :
    ## 1-  know the page you start from (by knowing the size of the file and then divide it by page size ) (done)
    ## 2- make only one place to read the page and one place to write in the page (to make it easier to handle the cache and the flushing to disk )(done)
    ## 3- make using the number of consumed pages instead of file_pointer (done) . 
    ## 4 - make it support writing multiple times and not overriding the old data (not done) .
    def __init__(self , file_path  , schema:Schema) : 
        self.file_path = file_path 
        self.data = None
        self.schema = schema 
        self.create_file_if_not_exists()
    
    def create_file_if_not_exists(self) :
        Path(self.file_path).touch(exist_ok=True)

    def get_last_page_index(self) : 
        """
        main functionality : get the current page index in the file  (-1 if the file is empty)
        """
        file_size = os.path.getsize(self.file_path)
        last_page_index = (file_size / PAGE_SIZE) - 1
        last_page_index = int(last_page_index)
        assert file_size % PAGE_SIZE == 0, f'the file size is {file_size} should  multiple of PAGE_SIZE'
        return last_page_index

    def get_number_of_row(self) : 
        """
        main :  get the number of rows in the current page 
        """
        current_page = self.get_data()
        return int.from_bytes(current_page[0:2])
    
    def get_data(self):
        """
        main functionality : get the right current page 
        1 - check if the self.data is None then read it from the file . 
        2 - check if the file is empty (create the first page). 
        3 - update the self.data (cached) to be used .
        """ 
        if self.data is None : 
            current_page_ptr = self.get_last_page_index() * PAGE_SIZE
            if current_page_ptr < 0 : 
                self.initialize_page() 
                current_page_ptr = self.get_last_page_index() * PAGE_SIZE
            with open(self.file_path ,"rb") as f:
                f.seek(current_page_ptr)
                self.data = bytearray(f.read(PAGE_SIZE))
        return self.data

    def write_data(self, start_index, end_index, value) :
        """
        main functionalty  : it will get the newest page using get_data and then update it . 
        note : end_index is exclusive
        """
        assert start_index <= end_index and end_index <=PAGE_SIZE , "wronge values for indexes in page writing  "
        current_data = self.get_data()
        current_data[start_index:end_index] = value
        self.data = current_data 
        return current_data

    def update_page_header(self , refernce : int) : 
        """
        main functionality : it update the page header 
        1 - update the number of rows 
        2 - insert a refernce index for the new page 
        """
        number_of_rows = self.get_number_of_row() + 1
        self.write_data(0, 2, number_of_rows.to_bytes(2))
        self.write_data(number_of_rows*2, number_of_rows*2+2, refernce.to_bytes(2))

    def get_new_ranges(self , length_of_row) : 
        """
        main functionality : get the indices of the new row 
        """
        reference_index = self.get_number_of_row()*2
        current_data  = self.get_data()
        last_index_used = int.from_bytes(current_data[reference_index:reference_index+2])
        if last_index_used == 0 : 
           last_index_used = PAGE_SIZE  
        return {'start': last_index_used - length_of_row, 'end': last_index_used}
    
    def initialize_page(self):
        """
        main functionality : initialize a new page in the heap file 
        actions : 
        1 -  get last index of current index (zero if the file is empty). 
        2 -  aquire a new page in the file . 
        3 -  invalidate the chached data 
        """
        data = bytearray(PAGE_SIZE)
        new_page_index = self.get_last_page_index()*PAGE_SIZE + PAGE_SIZE
        with open(self.file_path , 'ab') as f :
            f.seek(new_page_index)
            f.write(data)
        self.data = None 
        return data
        
        
    def next(self) : 
        row = self.childs[0].next()
        if row is None : 
            self.flush_on_disk()
            return None
        encoded_row = encodingCsv.encode_row(row , self.schema)
        ranges = self.get_new_ranges(len(encoded_row))
        if (self.get_number_of_row()+2)*2 > ranges['start']: 
            self.flush_on_disk()
            self.initialize_page()
            ranges = self.get_new_ranges(len(encoded_row))
        self.write_data(ranges['start'], ranges['end'], encoded_row)
        self.update_page_header(ranges['start'])
        return row

    def flush_on_disk(self) :
        Path(self.file_path).touch(exist_ok=True)
        with open(self.file_path , 'r+b') as f : 
            last_page_index = self.get_last_page_index()    
            f.seek(last_page_index* encodingCsv.PAGE_SIZE)
            f.write(self.data)
class NestedLoopJoin(object) :
    def __init__(self):
        self.left_value:dict = None 
    
    def next(self):
        assert self.childs[0] or self.childs[1] , 'the join must have atleast two childrens' 

        ## if first is none get it , always get the second one , if second one is None reset it and call both first and second one  
        right_value = self.childs[1].next()
        if right_value is None : 
           self.left_value = None
           self.childs[1].reset() 
           right_value = self.childs[1].next()

        if self.left_value is None : 
           self.left_value =  self.childs[0].next()
        
        if self.left_value is None or right_value is None :
            return None
        
        result = self.left_value.copy()
        result.update(right_value)
        return result

    def reset(self):
        self.childs[0].reset()
        self.childs[1].reset()

# 1 -  make a test plan 
#   -  make a memory scan memek a rating and movies the tha rating some movies will not have a rating we should assert that the number of rows is the numbwer of rating rows have movies in movies table 
# 2 - make the structure of the class 
#.    -  will have a function that take the un-hashed table and create a key to search for the hashed table , a second one that take a row and create a hashed key to be saved in dictionary DS 
# 3 - implement the logic 
# 4 - implemente the test 
# 5 - run the test of it 
# 6 -  watch the vidio 
# 
class HashJoin(object):
    def __init__(self, get_hash_key_hashed_table, get_hash_key_iterated_table) :
        """
         this function take two functions as attributes 
           -  get_hash_key_hashed_table
           -  get_hash_key_iterated_table
           then initialeze a hashed table (dict[key,list])
           we always assume that child0 is the table will be hashed  child1 is that one that will be iterated (it preferable that the left one is the smaller one )
        """
        self.get_hash_key_hashed_table = get_hash_key_hashed_table
        self.get_hash_key_iterated_table = get_hash_key_iterated_table
        self.hashed_table = defaultdict(list)
        self.is_hashed = 0 
        self.buffered_array = None
        self.right_row = None
     
    def hash_the_table(self):
        """
        this function will iterate through the hashable table and hash it using the get_hash_key_hashed_table
        """     
        while True: 
            row = self.childs[0].next()
            if row is None : 
               break 
            hash_key = self.get_hash_key_hashed_table(row)
            self.hashed_table[hash_key].append(row)

        self.is_hashed = 1 
             

    def get_buffered_row(self):
        
        if self.buffered_array is not None and len(self.buffered_array) > 0 : 
            row = self.buffered_array.pop()
            if len(self.buffered_array) == 0 : 
               self.buffered_array = None
            return  row 
        return None
    
    def next(self):
        if self.is_hashed == 0 : 
           self.hash_the_table()  
        
        while True :
            if self.right_row is None :  
                self.right_row  = self.childs[1].next()
            
                if self.right_row is None : 
                    return None 
                hash_key = self.get_hash_key_iterated_table(self.right_row)
                self.buffered_array = self.hashed_table.get(hash_key,[]).copy()
            left_row = self.get_buffered_row()
            
            if left_row is not None :
               result = left_row.copy()
               result.update(self.right_row)
               return result
            else : 
                self.right_row = None 

            
    
    def reset(self) :
        self.childs[0].reset()
        


def QueryBuilder(nodes : list , parent:list) : 
    """
    constract a tree plan ,  parent list assumes the nodes are zero indexed
    """
    # TODO :  make better names 
    root  = None 
    for i , parent_pointer in enumerate(parent) : 
        if parent_pointer !=-1 : 
           nodes[parent_pointer].childs = []     

    for i, parent_pointer  in  enumerate(parent) : 
        if parent_pointer != -1 :
           nodes[parent_pointer].childs.append(nodes[i])
        else : 
           root = nodes[i] 
    assert root is not None , 'the root should not be none'
    return root

def run(q):
    """
    Run the given query to completion by calling `next` on the (presumed) root
    """
    while True:
        x = q.next()
        # print(f"got {x} from node {type(q).__name__}")
        if x is None:
            break
        yield x



def test_memory_query_exceuter() : 
    birds = (
        ('amerob', 'American Robin', 0.077, True),
        ('baleag', 'Bald Eagle', 4.74, True),
        ('eursta', 'European Starling', 0.082, True),
        ('barswa', 'Barn Swallow', 0.019, True),
        ('ostric1', 'Ostrich', 104.0, False),
        ('emppen1', 'Emperor Penguin', 23.0, False),
        ('rufhum', 'Rufous Hummingbird', 0.0034, True),
        ('comrav', 'Common Raven', 1.2, True),
        ('wanalb', 'Wandering Albatross', 8.5, False),
        ('norcar', 'Northern Cardinal', 0.045, True)
    )
    schema = (
        ('id', str),
        ('name', str),
        ('weight', float),
        ('in_us', bool),
    )

    # ids of non US birds
    assert tuple(run(QueryBuilder([
        Projection(lambda x: (x[0],)),
        Selection(lambda x: not x[3]),
        MemoryScan(birds)
    ], [-1, 0, 1]))) == (
        ('ostric1',),
        ('emppen1',),
        ('wanalb',),
    )
    
    # id and weight of 3 heaviest birds
    assert tuple(run(QueryBuilder([
        Projection(lambda x: (x[0], x[2])),
        Limit(3),
        Sort(lambda x: x[2], desc=True),
        MemoryScan(birds),
    ], [-1, 0, 1, 2]))) == (
        ('ostric1', 104.0),
        ('emppen1', 23.0),
        ('wanalb', 8.5),
    )
    ## id and weight of 3 lightest birds skipping the lightest one
    assert tuple(run(QueryBuilder([
        Projection(lambda x: (x[0], x[2])),
        Limit(limit = 3, offset=1),
        Sort(lambda x: x[2], desc=False),
        MemoryScan(birds),
    ], [-1, 0, 1, 2]))) == (
        ('barswa', 0.019),
        ('norcar', 0.045),
        ('amerob', 0.077),
    )
    ## test the group by and aggregation sum of the third field (weight)

    assert tuple(run(QueryBuilder([
        GroupBy(lambda x: x[3], lambda key, records: (key, sum(r[2] for r in records))),
        Sort(lambda x: x[3]),
        MemoryScan(birds),
    ], [-1, 0, 1]))) == (
        (False, 135.5),
        (True, 6.1664),
    ) 
    print('ok') 



def test_group_by_node() : 
    file_path = '' 
    movie_csv_schema = (
        ('movieId', int),
        ('title', str),
        ('genres', str),
    )
    x = tuple(run(

        QueryBuilder([  GroupBy(lambda _: 1, lambda key, records: (key, len(records)) ),
            Selection(lambda x: x[0]%10==0 ),
            Limit(100),
            CSVScan(csv_file_path, movie_csv_schema)
        ], [-1, 0, 1, 2])
    ))

    assert x == ((1, 10),) , 'the value of x is : ' + str(x)
    print('ok')
def test_csv_file_rating() :
    csv_file_path = ''  # Path to your CSV file
    rating_csv_schema = (
        ('userId', int),
        ('movieId', int),
        ('rating', float),
        ('timestamp', int),
    )
    x = tuple(run(
        QueryBuilder([ 
            Selection(lambda x: x[0]%777==0 ),
            readWholeCSVFile(csv_file_path, rating_csv_schema)
        ], [-1, 0])
    ))

def test_insert_functionalty() : 
    file_path = 'test.bin'  
    Path.touch(file_path , exist_ok = True)  
    schema = [
        'int32',
        'text',
        'text'
    ]
    table = [
        (1, 'mohsen', 'magdy'), 
        (2, 'mohsen1', 'magdy'),
        (3, 'mohsen2', 'magdy'),
        (4, 'moshen3', 'magdy')
    ]
    linked_list = tuple(
        run(  
            QueryBuilder([Insert(file_path , schema),MemoryScan(table)], [-1,0])
        )
    )
    
    linked_list = tuple(
        run(
        QueryBuilder([
            Insert(file_path , schema),
            Limit(limit = 1024),
            HeapScan(file_path , schema)
        ], 
        [-1,0,1]
        )
    )
    ) 
    table_after_insert = tuple(
        run(
        QueryBuilder([HeapScan(file_path ,schema)],[-1])
        )
    )
    length_of_table_after_insert = len(table_after_insert)

def test_order_heap_file_reader() :
    csv_file_path = '/Users/ahmeali/Downloads/ml-20m/movies.csv'  # Path to your CSV file
    
    heap_file_path = csv_file_path.replace('csv' , 'bin')
    schema = [
        'int32' , 
        'text' , 
        'text' , 
    ]
    encodingCsv.convert_csv_file_to_binary_format(csv_file_path , schema)
    x = tuple(run(
        QueryBuilder([ 
            HeapScan(heap_file_path , schema)
        ], [-1])
    ))
    wanted_output = x[-2:]

def delete_files_after_test(file_paths:list):
    for file_path in file_paths : 
        if os.path.exists(file_path) :
           os.remove(file_path) 

    
if __name__ == '__main__':
     
    pass 

