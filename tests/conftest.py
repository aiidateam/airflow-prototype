import pytest
import os


@pytest.fixture(scope='session')
def aiida_profile():
    """Load and return the AiiDA profile for testing.

    The profile name is read from the AIIDA_PROFILE environment variable.
    This fixture is session-scoped, so the profile is loaded once per test session.

    Returns:
        Profile: The loaded AiiDA profile

    Raises:
        RuntimeError: If AIIDA_PROFILE is not set or profile cannot be loaded
    """
    from airflow_provider_aiida.aiida_core import load_profile

    profile_name = os.getenv('AIRFLOW_PROVIDER_AIIDA__TESTS__AIIDA_PROFILE')

    if profile_name is None:
        raise RuntimeError(
            "AIRFLOW_PROVIDER_AIIDA__TESTS__AIIDA_PROFILE environment variable is not set.\n"
            "This should be set automatically by the hatch-test environment.\n"
            "If running tests manually, set it with: export AIIDA_PROFILE=test"
        )

    try:
        profile = load_profile(profile_name)
    except Exception as e:
        print(f"\n✗ Failed to load AiiDA profile '{profile_name}'")
        print(f"Error: {e}")
        print("\nPlease create the test profile first:")
        print("  hatch run hatch-test.py3.11:setup-profile")
        raise

    return profile


@pytest.fixture(scope='session')
def bash_code(aiida_profile):
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
