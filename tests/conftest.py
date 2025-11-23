import pytest
from aiida import load_profile

load_profile()


@pytest.fixture(scope='session')
def bash_code():
    """Create and return the bash@localhost code for tests.

    This fixture automatically sets up:
    1. The localhost computer (if not exists)
    2. The bash@localhost code (if not exists)

    Returns the bash code that can be used in arithmetic calculations.
    """
    from shutil import which
    from aiida.common import exceptions
    from aiida.orm import InstalledCode, load_code

    try:
        code = load_code('bash@localhost')
    except exceptions.NotExistent:
        # Prepare localhost computer
        localhost = _prepare_localhost()

        # Create bash code
        code = InstalledCode(
            label='bash',
            computer=localhost,
            filepath_executable=which('bash'),
            default_calc_job_plugin='core.arithmetic.add',
        ).store()

    return code


def _prepare_localhost():
    """Prepare and return the localhost computer.

    If it doesn't already exist, the computer will be created using:
    - Transport: core.local
    - Scheduler: core.direct
    - Safe interval: 0 seconds (for fast testing)
    - Minimum job poll interval: 0 seconds (for fast testing)
    """
    import tempfile
    from aiida.common import exceptions
    from aiida.orm import Computer, load_computer

    try:
        computer = load_computer('localhost')
    except exceptions.NotExistent:
        computer = Computer(
            label='localhost',
            hostname='localhost',
            description='Localhost automatically created for tests',
            transport_type='core.local',
            scheduler_type='core.direct',
            workdir=tempfile.gettempdir(),
        ).store()
        computer.configure(safe_interval=0.0)
        computer.set_minimum_job_poll_interval(0.0)

    if not computer.is_configured:
        computer.configure()

    return computer
