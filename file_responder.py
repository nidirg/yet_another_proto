import hashlib
import secrets
import urllib
from shutil import copyfile
from worker import Worker
import os


def check_file(file):
    return os.path.exists(file) and os.path.getsize(file) > 0


def md5_file(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def save_file(file_dir, file_data=bytes(), file_name='', url=''):
    if url:
        if not file_name:
            path = urllib.parse.urlparse(url).path
            file_name = os.path.basename(path)
        file_name, headers = urllib.request.urlretrieve(url, filename=file_name)

    if not os.path.exists(file_dir):
        os.mkdir(file_dir)

    temp_file_name = os.path.join(file_dir, secrets.token_hex(8) + '.dat')
    if file_data:
        f = open(temp_file_name, 'wb')
        f.write(file_data)
        f.close()
    elif file_name:
        copyfile(file_name, temp_file_name)

    file_hash = md5_file(temp_file_name)
    ext = os.path.splitext(file_name)[1]
    new_name = file_hash + ext
    if not os.path.isfile(os.path.join(file_dir, new_name)):
        os.rename(temp_file_name, os.path.join(file_dir, new_name))
    return new_name


class FileResponder(Worker):
    def __init__(self, controller, request, service_name, file_dir):
        Worker.__init__(self, controller, request, service_name)
        self.file_dir = file_dir

    def process(self, target_type, target_value):
        try:
            with open(os.path.join(self.file_dir, target_value), 'rb') as file:
                data = file.read()
                self.response(binary=data)
        except FileNotFoundError:
            self.response(success=False, errors=[{
                'code': 1,
                'message': f'File {target_value } is not found.'
            }])

    def stop(self):
        pass

    def can_process(self, target_type, target_value):
        if check_file(os.path.join(self.file_dir, target_value)):
            return True, []
        else:
            return False, [{
                'code': 1,
                'message': f'File {target_value } is not found.'
            }]

