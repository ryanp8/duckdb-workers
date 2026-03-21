import hashlib
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
        self._workers_conns = {}
        for worker in workers:
            self.add_worker(worker)
        self.md5 = hashlib.md5()
        self.lock_manager = LockManager()
    
    def add_worker(self, location):
        conn = pyarrow.flight.connect(location)
        self._workers_conns[location] = conn
        
    def _select_worker(self, files):
        target = files[0]
        largest = ''
        selected_worker = None
        for location in self._workers_conns:
            combined = target + location
            self.md5.update(combined.encode())
            if self.md5.hexdigest() > largest:
                largest = self.md5.hexdigest()
                selected_worker = self._workers_conns[location]
        return selected_worker


    def execute_read(self, files, query, args):
        self.lock_manager.add_rfiles(files)

        print(f'Executing read with query: {query} and args: {args} on files: {files}')
        worker = self._select_worker(files)
        ticket = {
            'type': 'read',
            'query': query,
            'args': args,
            'is_stolen': False
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
            'type': 'write',
            'query': query,
            'args': args,
            'is_stolen': False
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
    
    def clear_cache(self, worker_location):
        assert worker_location in self._workers_conns, f'Worker {worker_location} not found'
        worker = self._workers_conns[worker_location]
        worker.do_action(pa.flight.Action('clear_cache', b''))
    
    def clear_all_cache(self):
        for location in self._workers_conns:
            self.clear_cache(location)
    
    
if __name__ == "__main__":

    app = Flask(__name__)
    coordinator = Coordinator()
    
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
        
    @app.post('/clear_cache')
    def clear_cache():
        body = request.json
        worker_location = body.get('worker_location', None)
        try:
            if not worker_location:
                coordinator.clear_all_cache()
            else:
                coordinator.clear_cache(worker_location)
            return json.dumps({'result': 'Cache cleared successfully'})
        except Exception as e:
            print(f'Error clearing cache: {e}')
            return json.dumps({'error': str(e)}), 400
        

    app.run()