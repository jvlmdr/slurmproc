from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import pprint
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

    logger.info('SLURM_JOB_NODELIST=%s', os.environ.get('SLURM_JOB_NODELIST', ''))
    logger.info('CUDA_VISIBLE_DEVICES=%s', os.environ.get('CUDA_VISIBLE_DEVICES', ''))

    try:
        logger.debug('load func from "%s"', args.dir)
        func = util.load_func(args.dir)
        logger.debug('call func')
        output = func()
    except Exception as ex:
        logger.warning('exception: %s', str(ex))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Use format_exception_only() instead of str(ex).
        # Sometimes it gives more information (e.g. for assertion with no message).
        msg_lines = traceback.format_exception_only(exc_type, exc_value)
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        result = (None, (msg_lines, tb_lines))
        logger.debug('dump result to "%s"', args.dir)
        util.dump_result(result, args.dir)
        raise

    result = (output, None)
    logger.debug('dump result to "%s"', args.dir)
    util.dump_result(result, args.dir)


if __name__ == '__main__':
    # def trace(frame, event, arg):
    #     print("%s, %s:%d" % (event, frame.f_code.co_filename, frame.f_lineno))
    #     return trace
    # sys.settrace(trace)
    main()
