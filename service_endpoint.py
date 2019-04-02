import logging
import websocket
import time
import json
from enum import Enum
from file_responder import FileResponder

PROTOCOL_VERSION = 1


class State(Enum):
        ready = 0
        busy = 1


class BaseWebsocketClient:
    def __init__(self, server_address):
        self.server_address = server_address
        self.ws = None

    def on_message(self, message):
        pass

    def on_error(self, error):
        pass

    def on_close(self):
        pass

    def on_open(self):
        pass

    def write(self, data):
        if isinstance(data, str):
            self.ws.send(data)
        elif isinstance(data, (bytes, bytearray)):
            self.ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)

    def start(self):
        self.ws = websocket.WebSocketApp(self.server_address,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         on_open=self.on_open)
        while True:
            try:
                time.sleep(2)
                self.ws.run_forever()
            except Exception as e:
                print(e)


class ServiceEndpoint(BaseWebsocketClient):
    def __init__(self, address, service_name, target_types, worker_class):
        BaseWebsocketClient.__init__(self, address)
        self._state = State.ready
        self._worker = None
        self._message_counter = 0
        self.logger = logging.getLogger(service_name)
        self._service_name = service_name
        self._target_types = target_types
        self.worker_class = worker_class
        self.file_dir = 'files'

    def on_open(self):
        self.logger.info('Open connection')
        self.write(json.dumps({
            'type': 'connect',
            'id': self.generate_message_id(),
            'id_ack': 0,
            'service_name': self._service_name,
            'target_types': self._target_types,
            'status': self._state.name,
            'protocol_version': self.get_protocol_version()
            }))

    def generate_message_id(self):
        self._message_counter += 1
        return self._message_counter

    def get_protocol_version(self):
        return PROTOCOL_VERSION

    def cancel_request(self):
        if self._worker:
            self._worker.stop()
            self._worker = None

    def ready(self, flag):
        if flag:
            self._state = State.ready
        else:
            self._state = State.busy

    def on_message(self, message):
        self.logger.info(f'On message: {message}')
        request = json.loads(message)

        result = True
        worker = None
        errors = []
        if request['type'] == 'connect_ack':
            pass
        elif request['type'] == 'request':
            if request['target_type'] == 'file':
                worker = FileResponder(self, request, self._service_name, self.file_dir)
                result, errors = worker.can_process(request['target_type'], request['target_value'])
            elif self._state == State.ready:
                worker = self.worker_class(self, request, self._service_name)
                result, errors = worker.can_process(request['target_type'], request['target_value'])
                if result:
                    self._state = State.busy 
                    self._worker = worker
                else:
                    State.ready
            else:
                result = False
                errors.append({'code': 0, 'message': 'worker is not ready. Current status:' +
                              self._state.name})

            worker.send_ack(result, errors)
            if result and worker:
                worker.process_request(request)
        elif request['type'] == 'cancel':
            self.cancel_request()

    def send_status(self, message=''):
        status = {
            'type': 'status',
            'id': self.generate_message_id(),
            'id_ack': 0,
            'status': self.state(),
            'message': message
        }
        self.write(json.dumps(status))

    def on_error(self, error):
        self.logger.error(error)
        self.cancel_request()

    def on_close(self):
        self.logger.error('Close connection.')
        self.cancel_request()

    def state(self):
        return self._state.name

