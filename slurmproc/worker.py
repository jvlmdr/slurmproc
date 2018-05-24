from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import traceback

import logging
logger = logging.getLogger(__name__)

from . import util


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir')
    args = parser.parse_args()

    try:
        func = util.load_func(args.dir)
        output = func()
        result = (output, None)
    except Exception as ex:
        ex_traceback = traceback.format_exc()
        result = (None, ex_traceback)

    util.dump_result(result, args.dir)


if __name__ == '__main__':
    main()
