from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import subprocess
import tempfile
import time
import traceback

import logging
logger = logging.getLogger(__name__)

from . import util


class Process(object):

    def __init__(self, func, dir=None, tempdir=None, opts=None, setup_cmds=None,
                 output_filename='output.txt', error_filename='error.txt'):
        if not dir:
            assert tempdir
            if not os.path.exists(tempdir):
                os.makedirs(tempdir, 0755)
            dir = tempfile.mkdtemp(dir=tempdir)
        else:
            if os.path.exists(dir):
                raise RuntimeError('dir already exists')
        if not os.path.exists(dir):
            os.makedirs(dir, 0755)

        script_file = os.path.join(dir, 'script.sh')
        with open(script_file, 'w') as f:
            write_script(f, dir, opts=opts, setup_cmds=setup_cmds)
        util.dump_func(func, dir)
        job_id = _parse_job_id(subprocess.check_output([
            'sbatch',
            '--output={}'.format(os.path.join(dir, output_filename)),
            '--error={}'.format(os.path.join(dir, error_filename)),
            script_file]))
        logger.debug('started job %s in dir "%s"', str(job_id), dir)

        self._dir = dir
        self._job_id = job_id


    def poll(self):
        return poll(self._job_id)


    def wait(self, **kwargs):
        wait(self._job_id, **kwargs)
        result = util.load_result(self._dir)
        output, err = result
        if err is not None:
            try:
                msg, tb = err
            except:
                raise RuntimeError('cannot unpack error: {}'.format(repr(ex)))
            raise RuntimeError('error in slurm process: {}\n\n{}'.format(msg, tb))
        return output


def call(func, **kwargs):
    return Process(func, **kwargs).wait()


def write_script(f, dir, opts=None, setup_cmds=None):
    opts = opts or []
    setup_cmds = setup_cmds or []
    script = SCRIPT_TEMPLATE.format(
        opts='\n'.join(['#SBATCH ' + opt for opt in opts]),
        setup_cmds='\n'.join(setup_cmds),
        dir=dir)
    f.write(script)


SCRIPT_TEMPLATE = '''\
#!/bin/bash
{opts}
{setup_cmds}
python -m slurmproc.worker {dir}
'''


def _parse_job_id(out):
    match = re.match('Submitted batch job (\d+)', out)
    if not match:
        raise RuntimeError('could not read job number from stdout: \'{}\''.format(out))
    return int(match.group(1))


def poll(job_id):
    job_id = int(job_id)
    status = poll_all()
    return status.get(job_id, None)
    # out = subprocess.check_output(['squeue', '--jobs={}'.format(job_id),
    #                                '--noheader', '--format=%t'])
    # out = out.strip()
    # if '\n' in out:
    #     raise RuntimeError('multiple lines in output: \'{}\''.format(out))
    # # logger.debug('poll state of job %s: \'%s\'', str(job_id), out)
    # return out


def wait(job_id, period=1):
    while True:
        state = poll(job_id)
        if not state:
            return
        if state not in ['PD', 'R', 'CG']:
            raise RuntimeError('unexpected state: {}'.format(state))
        time.sleep(period)


def poll_all():
    out = subprocess.check_output(['squeue', '--noheader', '--format=%A %t'])
    lines = out.splitlines()
    status = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        words = line.split(' ')
        if len(words) != 2:
            continue
        job_id, job_status = words
        status[int(job_id)] = job_status
    return status
