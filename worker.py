import json
import threading


class Worker:
    def __init__(self, endpoint, request, service_name):
        self._endpoint = endpoint
        self._request = request
        self._service_name = service_name

    def process_request(self, request):
        self._request = request
        thread = threading.Thread(target=self.thread_func, args=())
        thread.daemon = True
        thread.start()

    def thread_func(self):
        self.process(self._request['target_type'], self._request['target_value'])

    def process(self, target_type, target_value):
        """Must be override in user class.
            Process current request"""
        pass

    def stop(self):
        """Must be override in user class.
        Cancel request that are being processed"""
        pass

    def can_process(self, target_type, target_value):
        """Must be override in user class.
        Check is it possible to process target data"""
        pass

    def response(self, service_data='', last=True, success=True, file_list=[],
                 errors=[], binary=bytes()):
        try:
            if last:
                self._endpoint.ready(True)

            resp = {'type': 'response',
                    'id': self._endpoint.generate_message_id(),
                    'id_ack': self._request['id'],
                    'target_type': self._request['target_type'],
                    'target_value': self._request['target_value'],
                    'status': self._endpoint.state(),
                    'complete': last
                    }
            if self._request['target_type'] == 'file':
                resp['file_id'] = self._request['target_value']

            if success:
                resp['result'] = 'success'
                if service_data:
                    resp['service_data'] = service_data
                    resp['files'] = file_list
                elif binary:
                    meta_data = json.dumps(resp).encode()
                    resp = len(meta_data).to_bytes(4, byteorder='little') + meta_data + binary
            else:
                resp['result'] = 'error'
                resp['errors'] = errors

            self._endpoint.write(resp if binary else json.dumps(resp))

        except Exception as e:
            print('Error in response:', e)

    def send_ack(self, result, errors=[]):
        ack = {'type': 'request_ack',
               'id': self._endpoint.generate_message_id(),
               'id_ack': self._request['id'],
               'status': self._endpoint.state(),
               'result': 'success' if result else 'error',
               'errors': errors}
        self._endpoint.write(json.dumps(ack))
