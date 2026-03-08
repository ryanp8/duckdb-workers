from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.flight
import json

class LockManager:
    
    def __init__(self):
        self.rfiles = set()
        self.wfiles = set()
        
    

class Coordinator:
    
    def __init__(self, threads=8,workers=[]):
        self._workers_conns = workers
        self.pool = ThreadPoolExecutor(max_workers=threads)
        self.lock_manager = LockManager()
    
    def add_worker(self, location):
        conn = pyarrow.flight.connect(location)
        self._workers_conns.append(conn)
        
    def _select_worker(self, files):
        return self._workers_conns[0]

    def execute_read(self, files, query, args):
        for file in files:
            self.lock_manager.rfiles.add(file)

        worker = self._select_worker(files)
        ticket = {
            'query': query,
            'args': args
        }
        reader = worker.do_get(pa.flight.Ticket(json.dumps(ticket)))
        return reader.read_all()
    
    def execute_write(self, files, query, args):
        for file in files:
            self.lock_manager.wfiles.add(file)

        worker = self._select_worker(files)
        ticket = {
            'query': query,
            'args': args
        }
        reader = worker.do_put(pa.flight.Ticket(json.dumps(ticket)))
        return reader.read_all()
    
    
if __name__ == "__main__":
    coordinator = Coordinator()
    coordinator.add_worker('grpc://localhost:5005')
    coordinator.add_worker('grpc://localhost:5006')

    # result = coordinator.execute('', 'select ?', [10])
    
    query = "SELECT * FROM read_parquet('s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/part-00000-833f1ffd-f3fb-4221-b2be-1a71c47c95c3-c000.snappy.parquet');"
    # query = 'hi'
    result = coordinator.execute([], query, [])
    print(result)