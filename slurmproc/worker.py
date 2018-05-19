from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import pickle
import traceback

import logging
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir')
    args = parser.parse_args()

    func_file = os.path.join(args.dir, 'func.pickle')
    result_file = os.path.join(args.dir, 'result.pickle')

    try:
        with open(func_file, 'r') as f:
            func = pickle.load(f)
        output = func()
        result = (output, None)
    except Exception as ex:
        ex_traceback = traceback.format_exc()
        result = (None, ex_traceback)

    with open(result_file, 'w') as f:
        pickle.dump(result, f)


if __name__ == '__main__':
    main()
