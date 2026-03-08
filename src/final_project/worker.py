import random
import duckdb
import pyarrow as pa
import pyarrow.flight
import argparse

from dataclasses import dataclass
from queue import Empty, Queue
from threading import Condition, Lock, Thread
from flask import Flask, request


from flight_server import FlightServer, Task

@dataclass
class Config:
    AWS_KEY_ID: str
    AWS_SECRET_KEY: str
    AWS_REGION: str

class Worker:
    
    def __init__(self, location='grpc://localhost:5005'):
        self._location = location
        self.peers = {}
        self.config = Config('', '', '')
        self.task_queue: Queue[Task] = Queue()
        self.completed_tasks = {}
        self.cv = Condition(Lock())
        self.worker_thread = None
        self.flight_server = None
        self.flight_server_thread = None
        
    def _init_duckdb(self):
        con = duckdb.connect()
        con.execute('''
        INSTALL httpfs;
        LOAD httpfs;
        CREATE SECRET secret (
            TYPE s3,
            KEY_ID ?,
            SECRET ?,
            REGION ?
        );
        ''', (self.config.AWS_KEY_ID, self.config.AWS_SECRET_KEY, self.config.AWS_REGION))
        return con
    
    def add_peer(self, peer_location):
        self.peers[peer_location] = pa.flight.connect(peer_location)
        
    def remove_peer(self, peer_location):
        self.peers.remove(peer_location)

    def start(self):
        print(f'Starting worker at location:{self._location}')
        assert self.config.AWS_KEY_ID and self.config.AWS_SECRET_KEY and self.config.AWS_REGION, 'AWS credentials not set in config'

        con = self._init_duckdb()
        self.flight_server = FlightServer(self._location, self.cv, self.task_queue, self.completed_tasks, self.peers)
        self.flight_server_thread = Thread(target=self.flight_server.serve).start()
        self.worker_thread = Thread(target=self._do_work, args=(con,)).start()
        
    def _do_work(self, con):
        while True:
            try:
                task = self.task_queue.get(timeout=5)
                result = con.execute(task.query, task.args).fetch_arrow_table()
                with self.cv:
                    self.completed_tasks[task] = result
                    self.cv.notify_all()
            except Empty:
                print('no task found')
                Thread(target=self._steal_work).start()
            
    def _steal_work(self):
        if self.peers:
            peer_con = self.peers[random.sample(sorted(self.peers), 1)[0]]
            try:
                peer_con.do_action(pa.flight.Action('give_work', self._location.encode('utf-8')))
            except Exception as e:
                print(e)
                pass
        
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--flight_port', type=int, default=5005)
    parser.add_argument('--flask_port', type=int, default=8000)
    
    args = parser.parse_args()
    print(args)
    
    worker = Worker(f'grpc://localhost:{args.flight_port}')
    app = Flask(__name__)
    
    @app.get('/')
    def hello():
        return 'hello world'
    
    @app.post('/config')
    def config():
        json = request.get_json()
        worker.config = Config(**json)
        worker.start()
        return '', 200
    
    @app.post('/start')
    def start():
        try:
            worker.start()
        except AssertionError as e:
            return str(e), 400
        return '', 200
    
    @app.post('/peer')
    def add_peer():
        json = request.get_json()
        peer_location = json['location']
        worker.add_peer(peer_location)
        return '', 200
    
    @app.delete('/peer')
    def remove_peer():
        json = request.get_json()
        peer_location = json['location']
        try:
            worker.remove_peer(peer_location)
            return '', 200
        except KeyError:
            return 'Peer not found', 404

    app.run(port=args.flask_port)