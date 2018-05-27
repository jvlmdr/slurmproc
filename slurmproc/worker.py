from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import sys
import traceback

import logging
logger = logging.getLogger(__name__)

from . import util


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)  # TODO: Allow control?

    try:
        logger.debug('load func from "%s"', args.dir)
        func = util.load_func(args.dir)
        logger.debug('call func')
        output = func()
    except Exception as ex:
        logger.warning('exception in worker: %s', str(ex))
        msg = str(ex)
        tb = traceback.format_exc()
        result = (None, (msg, tb))
        logger.debug('dump result to "%s"', args.dir)
        util.dump_result(result, args.dir)
        raise

    result = (output, None)
    logger.debug('dump result to "%s"', args.dir)
    util.dump_result(result, args.dir)


if __name__ == '__main__':
    main()
