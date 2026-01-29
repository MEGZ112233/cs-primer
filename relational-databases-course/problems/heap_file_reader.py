import csv
import encodingCsv

class CSVScan(object):
    """
    Yield all records from the given "table" in memory.

    This is really just for testing... in the future our scan nodes
    will read from disk.
    """
    def __init__(self, filepath , schema = None):
        self.filepath = filepath
        self.offset = 0
        self.schema = schema
        with open(self.filepath, 'rb') as f:
            f.seek(0)
            header_line = f.readline()
            self.header = csv.reader([header_line.decode('utf-8')]).__next__()
            self.offset = f.tell()

    def next(self):
        with open(self.filepath , 'rb') as f:
            try : 
                f.seek(self.offset)
                row = f.readline()
                row = csv.reader([row.decode('utf-8')]).__next__()
                if not row:
                    return None
                self.offset = f.tell()
                if  self.schema :
                    for i in range(len(self.schema)):
                        _, field_type = self.schema[i]
                        row[i] = field_type(row[i])
                return tuple(row)
            except Exception as e:
                print("Error reading file:", e)
                return None

class HeapScan(object):
    def __init__(self , filepath , schema = None) : 
        self.filepath = filepath
        self.schema = schema 
        self.remaining_rows_in_page = 0
        self.number_of_consumed_pages = 0 
        self.current_pointer = 0
        self.finished = False 
    def initalize_page(self) : 
        page_start_pointer = self.number_of_consumed_pages * encodingCsv.PAGE_SIZE
        with open(self.filepath , 'rb') as f : 
            f.seek(page_start_pointer)
            self.remaining_rows_in_page = int.from_bytes(f.read(2) , 'big')
            self.current_pointer = f.tell()
        self.number_of_consumed_pages += 1
        if self.remaining_rows_in_page == 0 : 
            self.finished = True
    def next(self) : 
        if self.finished : 
            return None 
        if self.remaining_rows_in_page == 0 : 
            self.initalize_page()
            if self.finished : 
                return None 
        with open(self.filepath , 'rb') as f :
             try : 
                f.seek(self.current_pointer)
                row = encodingCsv.decode_row(f , self.schema)
                self.current_pointer = f.tell()
                self.remaining_rows_in_page -= 1
                return row
             except Exception as e : 
                print('Error reading from heap file : ' , e)
                return None

class readWholeCSVFile(object):
    def __init__(self, filepath , schema = None):
        self.filepath = filepath
        self.schema = schema
        self.table = []
        self.index = 0 
        with open(self.filepath , 'r' , newline='' , encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
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
    def __init__(self, table):
        self.table = table
        self.idx = 0

    def next(self):
        if self.idx >= len(self.table):
            return None

        x = self.table[self.idx]
        self.idx += 1
        return x
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
        self.group_keys = None
        self.idx = 0

    def next(self):
        if self.groups is None:
            # read all child records and group them
            self.groups = {}
            while True:
                x = self.child.next()
                if x is None:
                    break
                key = self.key_func(x)
                if key not in self.groups:
                    self.groups[key] = []
                self.groups[key].append(x)
            self.group_keys = list(self.groups.keys())

        if self.idx >= len(self.group_keys):
            return None

        key = self.group_keys[self.idx]
        records = self.groups[key]
        self.idx += 1
        result = self.agg_func(key, records)
        return result

class Projection(object):
    """
    Map the child records using the given map function, e.g. to return a subset
    of the fields.
    """
    def __init__(self, proj):
        self.proj = proj

    def next(self):
        x = self.child.next()
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
            x = self.child.next()
            if x is None or self.predicate(x):
                return x
            


class Limit(object):
    """
    Return only as many as the limit, then stop
    """
    def __init__(self, limit ,  offset=0):
        self.limit = limit
        self.offset = offset

    def next(self):
        x = self.child.next()
        if x is None or self.limit  <= 0:
            return None
        if self.offset  > 0 :
            self.offset  -= 1
            return self.next()
        self.limit  -= 1
        return x


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
        
    def next(self):
        if not self.sorted:
            while True:
                x = self.child.next()
                if x is None:
                    break
                if self.arr is None:
                    self.arr = []
                self.arr.append(x)
            self.arr = sorted(self.arr, key=self.key, reverse=self.desc)
            self.sorted = True
        if self.arr is None or self.idx >= len(self.arr):
            return None
        x = self.arr[self.idx]
        self.idx += 1
        return x


def Q(*nodes):
    """
    Construct a linked list of executor nodes from the given arguments,
    starting with a root node, and adding references to each child
    """
    ns = iter(nodes)
    parent = root = next(ns)
    for n in ns:
        parent.child = n
        parent = n
    return root


def run(q):
    """
    Run the given query to completion by calling `next` on the (presumed) root
    """
    while True:
        x = q.next()
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
    assert tuple(run(Q(
        Projection(lambda x: (x[0],)),
        Selection(lambda x: not x[3]),
        MemoryScan(birds)
    ))) == (
        ('ostric1',),
        ('emppen1',),
        ('wanalb',),
    )
    
    # id and weight of 3 heaviest birds
    assert tuple(run(Q(
        Projection(lambda x: (x[0], x[2])),
        Limit(3),
        Sort(lambda x: x[2], desc=True),
        MemoryScan(birds),
    ))) == (
        ('ostric1', 104.0),
        ('emppen1', 23.0),
        ('wanalb', 8.5),
    )
    ## id and weight of 3 lightest birds skipping the lightest one
    assert tuple(run(Q(
        Projection(lambda x: (x[0], x[2])),
        Limit(limit = 3, offset=1),
        Sort(lambda x: x[2], desc=False),
        MemoryScan(birds),
    ))) == (
        ('barswa', 0.019),
        ('norcar', 0.045),
        ('amerob', 0.077),
    )
    ## test the group by and aggregation sum of the third field (weight)

    assert tuple(run(Q(
        GroupBy(lambda x: x[3], lambda key, records: (key, sum(r[2] for r in records))),
        Sort(lambda x: x[3]),
        MemoryScan(birds),
    ))) == (
        (False, 135.5),
        (True, 6.1664),
    ) 
    print('ok') 


def test_csv_file_movie_reader() : 
    csv_file_path = ''  # Path to your CSV file
    movie_csv_schema = (
        ('movieId', int),
        ('title', str),
        ('genres', str),
    )
    x = tuple(run(

        Q(  GroupBy(lambda _: 1, lambda key, records: (key, len(records)) ),
            Selection(lambda x: x[0]%10==0 ),
            Limit(100),
            CSVScan(csv_file_path, movie_csv_schema)
        )
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
        Q( 
            Selection(lambda x: x[0]%777==0 ),
            readWholeCSVFile(csv_file_path, rating_csv_schema)
        )
    ))
    print(x)
def test_order_heap_file_reader() :
    csv_file_path = '/Users/ahmeali/cs-primer/relational-databases-course/problems/orders.csv' 
    # encodingCsv.convert_csv_file_to_binary_format(csv_file_path)
    heap_file_path = csv_file_path.replace('csv' , 'bin')
    order_heap_schema = {
        'id_order' : int, 
        'order_code' : str,
        'src_address' : str,
        'dst_address' : str,
        'total_cost' : float
    }
    x = tuple(run(
        Q( 
            HeapScan(heap_file_path , order_heap_schema)
        )
    ))
    print(x)
if __name__ == '__main__':
    # Test data generated by Claude and probably not accurate!
     
    # test_memory_query_exceuter()
    
    # test_csv_file_movie_reader()

    # test_csv_file_rating()
    test_order_heap_file_reader()
    pass 

