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
from .util import RemoteException


class Process(object):

    def __init__(self, func, dir=None, tempdir=None, opts=None, setup_cmds=None,
                 output_filename='output.txt', error_filename='error.txt'):
        '''Submits a job to slurm that computes func().

        Args:
            func: Must be able to be pickled. (Note partial() supports pickling.)
            dir: Directory in which to write scripts, inputs, outputs for job.
                If None, then create a temporary directory under tempdir.
                This directory (or tempdir) must be accessible by the worker (nfs).
            tempdir: Only used if dir is None.
            opts: List of strings like ['--time=1:00:00', '--partition=small']
            setup_cmds: Commands to execute in bash script before python.
        '''
        assert dir or tempdir, 'directory not specified'
        if not dir:
            if not os.path.exists(tempdir):
                os.makedirs(tempdir, 0o755)
            dir = tempfile.mkdtemp(dir=tempdir)
        else:
            if os.path.exists(dir):
                raise RuntimeError('dir already exists')
        if not os.path.exists(dir):
            os.makedirs(dir, 0o755)

        script_file = os.path.join(dir, 'script.sh')
        with open(script_file, 'w') as f:
            write_script(f, dir, opts=opts, setup_cmds=setup_cmds)
        util.dump_func(func, dir)
        job_id = _parse_job_id(subprocess.check_output([
            'sbatch',
            '--output={}'.format(os.path.join(dir, output_filename)),
            '--error={}'.format(os.path.join(dir, error_filename)),
            script_file]))
        logger.debug('started job "%s" in dir "%s"', job_id, dir)

        self._dir = dir
        self._job_id = job_id

    def job_id(self):
        return self._job_id

    def poll(self):
        return poll(self._job_id)

    def wait(self, **kwargs):
        wait(self._job_id, **kwargs)
        return self.output()

    def output(self):
        result = util.load_result(self._dir)
        output, err = result
        if err is not None:
            try:
                msg_lines, tb_lines = err
            except:
                raise RuntimeError('cannot unpack error: {}'.format(repr(err)))
            msg = ''.join(msg_lines).strip()
            if msg:
                msg = msg.splitlines()[0]
            tb_file = os.path.join(self._dir, 'traceback.txt')
            try:
                with open(tb_file, 'w') as f:
                    for line in tb_lines:
                        f.write(line)
            except IOError as ex:
                logger.warning('could not write traceback to file: %s', str(ex))
            raise RemoteException(msg, tb_file)
        return output

    def terminate(self):
        terminate(self._job_id)

    def dir(self):
        return self._dir

    def __str__(self):
        return '[job={} dir={}]'.format(self._job_id, self._dir)


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
    job_id = match.group(1)
    _assert_integer(job_id)
    return job_id


def poll(job_id):
    '''Returns None if job is not in queue.'''
    _assert_integer(job_id)
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


def wait_any(job_ids, period=1):
    if len(job_ids) == 0:
        raise RuntimeError('no jobs to wait for')
    while True:
        states = poll_all()
        completed = set(job_ids).difference(set(states.keys()))
        # logger.debug('waiting for {} jobs to finish'.format(len(job_ids)))
        if len(completed) > 0:
            return completed
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
        _assert_integer(job_id)
        status[job_id] = job_status
    return status


def terminate(job_id):
    state = poll(job_id)
    if not state:
        return
    logger.debug('scancel %s', job_id)
    try:
        subprocess.check_call(['scancel', job_id])
    except CalledProcessError as ex:
        logger.warning('scancel %s: %s', job_id, str(ex))


def _assert_integer(s):
    if s != str(int(s)):
        raise RuntimeError('not an integer: {}'.format(s))
