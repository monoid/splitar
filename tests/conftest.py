import pytest
import subprocess
from enum import Enum


class RunMode(Enum):
    DEBUG = ()
    RELEASE = ('--release',)


@pytest.fixture(params=(RunMode.DEBUG, RunMode.RELEASE))
def cargo_run(request):
    def run(args):
        return subprocess.run(
            ['cargo', 'run', '--quiet'] + list(request.param.value) + ['--'] + args,
            check=True, capture_output=True)
    return run
