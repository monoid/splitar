""" Test the --compress """
import os
import time
import pytest
import tarfile
import subprocess
from tests.test_dirs import DIRS

@pytest.fixture
def bin(tmpdir, monkeypatch):
    bindir = tmpdir.mkdir('bin')
    monkeypatch.setenv('PATH', str(bindir), prepend=os.pathsep)

    def save(name, content):
        exe = bindir.join(name)
        exe.write(content)
        exe.chmod(0o770)
        return exe
    return save

@pytest.fixture
def exclusive_compress(bin):
    return bin('locker', """#!/usr/bin/env python
import fcntl, os, sys

lock_path = os.environ['X_SPLITAR_TEST_LOCK']
with open(lock_path, 'w') as lock_file:
    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    # Now copy stdin to stdout as noop filter.
    # Can eat all memory...
    sys.stdout.write(sys.stdin.read())
""")


def test_exclusive_compress_is_sane(exclusive_compress, tmpdir, monkeypatch):
    lock = tmpdir.join('file.lock')
    lock.write('')
    monkeypatch.setenv('X_SPLITAR_TEST_LOCK', str(lock))
    ret = subprocess.run(
        str(exclusive_compress),
        check=True,
        input=b'hi',
        capture_output=True)
    assert ret.stdout == b'hi'


def test_exclusive_compress_is_exclusive(exclusive_compress, tmpdir, monkeypatch):
    lock = tmpdir.join('file.lock')
    lock.write('')
    monkeypatch.setenv('X_SPLITAR_TEST_LOCK', str(lock))
    proc1 = subprocess.Popen(
        [str(exclusive_compress)],
        stdin=subprocess.PIPE)
    proc2 = subprocess.Popen(
        [str(exclusive_compress)],
        stdin=subprocess.PIPE)
    time.sleep(0.1)
    # Both procs are waitng for the input... Except one who has no l(o|u)ck!
    proc2.communicate()
    proc1.communicate()
    assert proc1.returncode != 0 or proc2.returncode != 0
    assert proc1.returncode == 0 or proc2.returncode == 0


def test_basic(cargo_run, tmpdir):
    import gzip

    outdir = tmpdir.mkdir('out')
    output = outdir.join('output.tar.')
    inp = tmpdir.join('input.tar')

    with tarfile.open(str(inp), mode='w') as tar:
        for obj in DIRS.get_children(''):
            obj.add_to_tar(tar)
    # Test with gzip
    cargo_run([
        '-S', '30K', '--compress', 'gzip', '--recreate-dirs',
        str(inp), str(output),
    ])
    files = sorted(os.listdir(str(outdir)))
    assert files == ['output.tar.00000', 'output.tar.00001']

    for file in files:
        with open(str(outdir.join(file)), 'rb') as f:
            with gzip.open(f, 'rb') as g:
                g.read()


def test_failure(cargo_run, tmpdir):
    outdir = tmpdir.mkdir('out')
    output = outdir.join('output.tar.')
    inp = tmpdir.join('input.tar')

    with tarfile.open(str(inp), mode='w') as tar:
        for obj in DIRS.get_children(''):
            obj.add_to_tar(tar)
    with pytest.raises(subprocess.CalledProcessError):
        cargo_run([
            '-S', '30K', '--compress', 'false', '--recreate-dirs',
            str(inp), str(output),
        ])
    assert os.listdir(str(outdir)) == []


def test_completion(cargo_run, tmpdir, exclusive_compress, monkeypatch):
    lock = tmpdir.join('file.lock')
    lock.write('')
    monkeypatch.setenv('X_SPLITAR_TEST_LOCK', str(lock))
    # Test that subprocess is completed
    outdir = tmpdir.mkdir('out')
    output = outdir.join('output.tar.')
    inp = tmpdir.join('input.tar')

    with tarfile.open(str(inp), mode='w') as tar:
        for obj in DIRS.get_children(''):
            obj.add_to_tar(tar)
    # The exclusive_compress fails when two instances are launched.
    cargo_run([
        '-S', '30K', '--compress', str(exclusive_compress),
        '--recreate-dirs', str(inp), str(output),
    ])
