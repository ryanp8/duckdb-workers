import requests
import pyarrow as pa
import base64

class Client:
    def __init__(self, coordinator_url):
        self.coordinator_url = coordinator_url

    def _request(self, body, type):
        response = requests.post(f"{self.coordinator_url}/{type}", json=body)
        if response.ok:
            response_json = response.json()
            result_bytes = base64.b64decode(response_json["result"])
            buffer = pa.BufferReader(result_bytes).read_buffer()
            return pa.ipc.open_stream(buffer)
        else:
            print(f"Error: {response.status_code} - {response.text}")

    def read(self, body):
        return self._request(body, 'read')

    def write(self, body):
        return self._request(body, 'write')
        
    def clear_cache(self, worker_location=None):
        body = {}
        if worker_location:
            body['worker_location'] = worker_location
        response = requests.post(f'{self.coordinator_loc}/clear_cache', json=body)
        if response.ok:
            print('Cache cleared successfully')
        else:
            print(f'Error: {response.status_code} - {response.text}')
