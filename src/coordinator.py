from flask import Flask, request
from argparse import ArgumentParser
import hashlib
import pyarrow as pa
import pyarrow.flight
import json
import base64


class Coordinator:
    def __init__(self, workers=[]):
        print(workers)
        self._workers_conns = {}
        for worker in workers:
            self.add_worker(worker)
        self.md5 = hashlib.md5()

    def add_worker(self, location):
        print('adding peer', location)
        new_conn = pyarrow.flight.connect(location)
        for existing_loc, existing_conn in self._workers_conns.items():
            existing_conn.do_action(
                pa.flight.Action("add_peer", location.encode("utf-8"))
            )
            new_conn.do_action(
                pa.flight.Action("add_peer", existing_loc.encode("utf-8"))
            )
        self._workers_conns[location] = new_conn

    def _select_worker(self, files):
        target = files[0]
        largest = ""
        selected_worker = None
        for location in self._workers_conns:
            combined = target + location
            self.md5.update(combined.encode())
            digest = self.md5.hexdigest()
            if digest > largest:
                largest = digest
                selected_worker = self._workers_conns[location]
        return selected_worker

    def execute_read(self, files, query, args):

        print(f"Executing read with query: {query} and args: {args} on files: {files}")
        worker = self._select_worker(files)
        ticket = {"type": "read", "query": query, "args": args, "is_stolen": False}

        result = worker.do_get(pa.flight.Ticket(json.dumps(ticket)))
        reader = result.to_reader()
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            for batch in reader:
                writer.write_batch(batch)

        return sink.getvalue().to_pybytes()

    def clear_cache(self, worker_location):
        assert worker_location in self._workers_conns, (
            f"Worker {worker_location} not found"
        )
        worker = self._workers_conns[worker_location]
        worker.do_action(pa.flight.Action("clear_cache", b""))

    def clear_all_cache(self):
        for location in self._workers_conns:
            self.clear_cache(location)


if __name__ == "__main__":

    argparser = ArgumentParser()
    argparser.add_argument(
        "-w", "--workers", nargs="+"
    )
    args = argparser.parse_args()

    app = Flask(__name__)
    coordinator = Coordinator(args.workers)

    @app.post("/write")
    def write():
        body = request.json
        buckets = body["buckets"]
        query = body["query"]
        args = body["args"]
        try:
            result_bytes = coordinator.execute_write(buckets, query, args)
            return json.dumps({"result": base64.b64encode(result_bytes).decode()})
        except Exception as e:
            print(f"Error executing write: {e}")
            return json.dumps({"error": str(e)}), 400

    @app.post("/read")
    def read():
        body = request.json
        buckets = body["buckets"]
        query = body["query"]
        args = body["args"]
        try:
            result_bytes = coordinator.execute_read(buckets, query, args)
            return json.dumps({"result": base64.b64encode(result_bytes).decode()})
        except Exception as e:
            print(f"Error executing read: {e}")
            return json.dumps({"error": str(e)}), 400

    @app.post("/clear_cache")
    def clear_cache():
        body = request.json
        worker_location = body.get("worker_location", None)
        try:
            if not worker_location:
                coordinator.clear_all_cache()
            else:
                coordinator.clear_cache(worker_location)
            return json.dumps({"result": "Cache cleared successfully"})
        except Exception as e:
            print(f"Error clearing cache: {e}")
            return json.dumps({"error": str(e)}), 400

    app.run()
