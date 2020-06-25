from os.path import dirname, abspath, join
from os import walk
from io import StringIO
import sys

test_user_name = 'test_user_name'
test_user_password = 'test_user_password'

root_dir = dirname(dirname(dirname(abspath(__file__))))
test_data_dir = join(root_dir, 'features', 'test_data')


class CapturingStdOut(list):

    def __enter__(self):
        self._stdout_old, self._stderr_old = sys.stdout, sys.stderr
        self._stdout_new, self._stderr_new = StringIO(), StringIO()
        sys.stdout, sys.stderr = self._stdout_new, self._stderr_new
        return self

    def __exit__(self, *args):
        self.stdout_capture = self._stdout_new.getvalue().splitlines()
        self.stderr_capture = self._stderr_new.getvalue().splitlines()
        del self._stdout_new  # free up some memory
        del self._stderr_new  # free up some memory
        sys.stdout = self._stdout_old
        sys.stderr = self._stderr_old


def test_file(file_name):
    return join(test_data_dir, file_name)


def delete_database_data():
    from settings import ENVIRONMENT
    from app.models import Transaction
    if ENVIRONMENT == 'DEV':
        txs = Transaction.objects.all()
        for tx in txs:
            tx.delete()


def group_by_field(models, field):
    grouped = dict()
    for model in models:
        key = getattr(model, field)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(model)
    return grouped


def as_list(s):
    s = s.replace(' ', '')
    s = s.replace('[', '')
    s = s.replace(']', '')
    return s.split(',')


def list_files(path, mask_include=None, mask_exclude=None):
    include = '' if mask_include is None else mask_include
    for root, _, files in walk(path):
        for file in files:
            if include in file and (mask_exclude is None or mask_exclude not in file):
                yield join(root, file)


def test_files(dir_name, mask_include=None, mask_exclude=None):
    path = join(test_data_dir, dir_name)
    return list_files(path, mask_include, mask_exclude)
