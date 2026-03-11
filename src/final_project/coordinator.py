from threading import Lock

import pyarrow as pa
import pyarrow.flight
import json
import base64

from flask import Flask, request

class LockManager:
    
    def __init__(self):
        self.rfiles = set()
        self.wfiles = set()
        self.mu = Lock()
        
    def remove_rfiles(self, files):
        with self.mu:
            for file in list(files):
                self.rfiles.remove(file)
            
    def remove_wfiles(self, files):
        with self.mu:
            for file in list(files):
                self.wfiles.remove(file)

    def add_rfiles(self, files):
        print('adding rfiles: ', files)
        with self.mu:
            added = []
            for file in list(files):
                if file in self.wfiles:
                    for f in added:
                        self.rfiles.remove(f)
                    raise Exception(f'File {file} is currently being written to')
                self.rfiles.add(file)
        
    def add_wfiles(self, files):
        with self.mu:
            added = []
            for file in list(files):
                if file in self.wfiles or file in self.rfiles:
                    for f in added:
                        self.wfiles.remove(f)
                    raise Exception(f'File {file} is currently being read from')
                self.wfiles.add(file)

class Coordinator:
    
    def __init__(self, workers=[]):
        self._workers_conns = []
        for worker in workers:
            self.add_worker(worker)
        self.lock_manager = LockManager()
    
    def add_worker(self, location):
        conn = pyarrow.flight.connect(location)
        self._workers_conns.append(conn)
        
    def _select_worker(self, files):
        return self._workers_conns[0]

    def execute_read(self, files, query, args):
        self.lock_manager.add_rfiles(files)

        print(f'Executing read with query: {query} and args: {args} on files: {files}')
        worker = self._select_worker(files)
        ticket = {
            'query': query,
            'args': args
        }

        result = worker.do_get(pa.flight.Ticket(json.dumps(ticket)))
        reader = result.to_reader()
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            for batch in reader:
                writer.write_batch(batch)
        
        self.lock_manager.remove_rfiles(files)
        return sink.getvalue().to_pybytes()

    
    def execute_write(self, files, query, args):
        self.lock_manager.add_wfiles(files)

        worker = self._select_worker(files)
        ticket = {
            'query': query,
            'args': args
        }
        while query == 'loop':
            pass
        
        # Use do get to execute all queries (read and write) for now
        result = worker.do_get(pa.flight.Ticket(json.dumps(ticket)))
        reader = result.to_reader()
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            for batch in reader:
                writer.write_batch(batch)

        self.lock_manager.remove_wfiles(files)
        return sink.getvalue().to_pybytes()
    
    
if __name__ == "__main__":
    # conn = grpc.insecure_channel('grpc://localhost:5005')
    # conn.
    # coordinator = Coordinator()
    # coordinator.add_worker('localhost:5005')
    # coordinator.add_worker('localhost:5006')

    # result = coordinator.execute('', 'select ?', [10])
    
    # query = "SELECT * FROM read_parquet('s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/part-00000-833f1ffd-f3fb-4221-b2be-1a71c47c95c3-c000.snappy.parquet');"
    # # query = 'hi'
    # result = coordinator.execute([], query, [])
    # print(result)
    
    app = Flask(__name__)
    coordinator = Coordinator()
    coordinator.add_worker('grpc://localhost:5005')
    
    # query = 'select ?;'
    # args = [10]
    # result = coordinator.execute_read([], query, args)
    # print(result)
    
    @app.post('/write')
    def write():
        body = request.json
        buckets = body['buckets']
        query = body['query']
        args = body['args']
        try:
            result_bytes = coordinator.execute_write(buckets, query, args)
            return json.dumps({'result': base64.b64encode(result_bytes).decode()})
        except Exception as e:
            print(f'Error executing write: {e}')
            return json.dumps({'error': str(e)}), 400

    @app.post('/read')
    def read():
        body = request.json
        buckets = body['buckets']
        query = body['query']
        args = body['args']
        try:
            result_bytes = coordinator.execute_read(buckets, query, args)
            return json.dumps({'result': base64.b64encode(result_bytes).decode()})
        except Exception as e:
            print(f'Error executing read: {e}')
            return json.dumps({'error': str(e)}), 400

    app.run()