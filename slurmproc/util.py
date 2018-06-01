from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pickle
# import dill as pickle

EXT = '.pickle'


def dump_func(func, dir):
    func_file = os.path.join(dir, 'func' + EXT)
    with open(func_file, 'w') as f:
        pickle.dump(func, f)


def load_func(dir):
    func_file = os.path.join(dir, 'func' + EXT)
    with open(func_file, 'r') as f:
        return pickle.load(f)


def dump_result(result, dir):
    result_file = os.path.join(dir, 'result' + EXT)
    with open(result_file, 'w') as f:
        pickle.dump(result, f)


def load_result(dir):
    result_file = os.path.join(dir, 'result' + EXT)
    with open(result_file, 'r') as f:
        return pickle.load(f)


class RemoteException(Exception):

    def __init__(self, msg, tb_file=None):
        self.msg = msg
        self.tb_file = tb_file

    def __str__(self):
        msg = self.msg
        if self.tb_file:
            msg += ' (traceback written to "{}")'.format(self.tb_file)
        return msg
