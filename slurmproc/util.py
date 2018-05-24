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
