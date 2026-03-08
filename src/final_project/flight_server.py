from dataclasses import dataclass
from threading import Lock, Condition, Thread
from queue import Queue, Empty

import pyarrow as pa
import pyarrow.flight
import duckdb
import json
import random

@dataclass(frozen=True)
class Task:
    query: str
    args: tuple
    
@dataclass
class Config:
    AWS_KEY_ID: str
    AWS_SECRET_KEY: str
    AWS_REGION: str

class Worker(pa.flight.FlightServerBase):
    
    def __init__(self, location='grpc://localhost:5005'):
        super().__init__(location)
        self._location = location
        self.cv = Condition(Lock())
        self.task_queue = Queue()
        self.completed_tasks = {}
        self.config = Config('', '', '')
        self.peers = {}
        self.inited = False


    def do_get(self, context, ticket):
        assert self.inited, 'Server not inited'
        body = json.loads(ticket.ticket.decode('utf-8'))
        print(f'Server received request with body: {body}')
        query = body['query']
        args = body['args']
        task = Task(query, tuple(args))
        self.task_queue.put(task)

        with self.cv:
            self.cv.wait_for(lambda: task in self.completed_tasks)
            result = self.completed_tasks.pop(task)

        return pa.flight.RecordBatchStream(result)


    def list_actions(self, context):
        return [
            ('give_work', 'If queue is not empty, gives a task to the peer that made the request'),
            ('add_peer', 'Adds a peer to the worker'),
            ('remove_peer', 'Removes a peer to the worker'),
            ('update_config', 'Sets AWS config')
        ]


    def do_action(self, context, action):
        print(f'Server received action: {action.type}')
        print(action.type == 'init')
        if action.type == 'give_work':
            self.do_give_work(action.body.to_pybytes().decode('utf-8'))
        elif action.type == 'add_peer':
            self.do_add_peer(action.body.to_pybytes().decode('utf-8'))
        elif action.type == 'remove_peer':
            self.do_remove_peer(action.body.to_pybytes().decode('utf-8'))
        elif action.type == 'init':
            self.do_init(json.loads(action.body.to_pybytes().decode('utf-8')))


    def do_add_peer(self, peer_location):
        self.peers[peer_location] = pa.flight.connect(peer_location)


    def do_remove_peer(self, peer_location):
        self.peers.pop(peer_location)

 
    def do_init(self, config):
        print(f'initializing server with config: {config}')
        assert config.AWS_KEY_ID and config.AWS_SECRET_KEY and config.AWS_REGION, 'AWS credentials not set in config'
        
        self.inited = True
        con = self._init_duckdb(Config(**config))
        self.worker_thread = Thread(target=self._do_work, args=(con,)).start()


    def do_give_work(self, peer_location):
        peer_conn = self.peers[peer_location]
        try:
            task = self.task_queue.get(block=False)
            ticket = {
                'query': task.query,
                'args': task.args
            }
            result = peer_conn.do_get(pa.flight.Ticket(json.dumps(ticket)))
            with self.cv:
                    self.completed_tasks[task] = result.read_all()
                    self.cv.notify_all()
        except:
            print('no work to give')


    def _init_duckdb(self, config):
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
        ''', (config.AWS_KEY_ID, config.AWS_SECRET_KEY, config.AWS_REGION))
        return con


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
    server = FlightServer('grpc://localhost:5005')
    print('running server')
    server.serve()
    
