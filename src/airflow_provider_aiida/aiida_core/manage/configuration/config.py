from aiida.common.exceptions import EntryPointError, StorageMigrationError
from typing import Any
import logging
import secrets
import os
import psycopg
from pathlib import Path
from airflow_provider_aiida.aiida_core.manage.configuration._postgres_utils import PostgreSqlProfileCommands
from aiida.manage import Profile

logger = logging.getLogger(__name__)

# TODO as soon as this is in some formal schema of the storage_config, we do not need this
def verify_profile_supports_airflow(profile: Profile):
    """
    Verify that an AiiDA profile has the necessary Airflow configuration.
    """
    required_keys = [
        'airflow_database_username',
        'airflow_database_password',
        'airflow_database_name'
    ]

    storage_config = profile.storage_config
    missing_keys = [key for key in required_keys if key not in storage_config]

    if missing_keys:
        raise ValueError(
            f"Profile '{profile.name}' does not support Airflow. "
            f"Missing required configuration keys: {', '.join(missing_keys)}.\n"
            f"The profile must be created using the setup_aiida_profile() function "
            f"or manually configured with Airflow database settings."
        )

def get_airflow_home(profile: Profile) -> Path:
    from aiida.manage.configuration import get_config
    verify_profile_supports_airflow(profile)

    # Get Airflow home directory path
    config = get_config()
    aiida_config_dir = Path(config.dirpath)
    return aiida_config_dir / "airflow" / profile.name

def create_profile(
    self,
    name: str,
    storage_backend: str,
    storage_config: dict[str, Any],
    broker_backend: str | None = None,
    broker_config: dict[str, Any] | None = None,
    is_test_profile: bool = False,
) -> Profile:
    """Create a new profile and initialise its storage.

    NOTE: This is a copy from the function aiida.manage.configuration.config.Config::create_profile.
          Only one line has been changed to not remove the airflow configuration.

    :param name: The profile name.
    :param storage_backend: The entry point to the :class:`aiida.orm.implementation.storage_backend.StorageBackend`
        implementation to use for the storage.
    :param storage_config: The configuration necessary to initialise and connect to the storage backend.
    :param broker_backend: The entry point to the :class:`aiida.brokers.Broker` implementation to use for the
        message broker.
    :param broker_config: The configuration necessary to initialise and connect to the broker.
    :returns: The created profile.
    :raises ValueError: If the profile already exists.
    :raises TypeError: If the ``storage_backend`` is not a subclass of
        :class:`aiida.orm.implementation.storage_backend.StorageBackend`.
    :raises EntryPointError: If the ``storage_backend`` does not have an associated entry point.
    :raises StorageMigrationError: If the storage cannot be initialised.
    """
    from aiida.brokers import Broker
    from aiida.orm.implementation.storage_backend import StorageBackend
    from aiida.plugins.entry_point import load_entry_point

    if name in self.profile_names:
        raise ValueError(f'The profile `{name}` already exists.')

    try:
        storage_cls = load_entry_point('aiida.storage', storage_backend)
    except EntryPointError as exception:
        raise ValueError(f'The entry point `{storage_backend}` could not be loaded.') from exception
    else:
        if not issubclass(storage_cls, StorageBackend):
            raise TypeError(
                f'The `storage_backend={storage_backend}` is not a subclass of '
                '`aiida.orm.implementation.storage_backend.StorageBackend`.'
            )

    # NOTE: Only the line below has been commented out, to not remove airlfow settings
    #storage_config = storage_cls.Model(**(storage_config or {})).model_dump()

    if broker_backend is not None:
        try:
            broker_cls = load_entry_point('aiida.brokers', broker_backend)
        except EntryPointError as exception:
            raise ValueError(f'The entry point `{broker_backend}` could not be loaded.') from exception
        else:
            if not issubclass(broker_cls, Broker):
                raise TypeError(
                    f'The `broker_backend={broker_backend}` is not a subclass of `aiida.brokers.broker.Broker`.'
                )

    profile = Profile(
        name,
        {
            'storage': {
                'backend': storage_backend,
                'config': storage_config,
            },
            'process_control': {
                'backend': broker_backend,
                'config': broker_config,
            },
            'test_profile': is_test_profile,
        },
    )

    logger.info('Initialising the storage backend.')
    try:
        profile.storage_cls.initialise(profile)
    except Exception as exception:
        raise StorageMigrationError(
            f'Storage backend initialisation failed, probably because the configuration is incorrect:\n{exception}'
        )
    logger.info('Storage initialisation completed.')

    self.add_profile(profile)
    self.store()

    return profile


def create_psql_database_from_aiida_profile(
    profile_name: str,
    postgres_hostname: str,
    postgres_port: int,
    postgres_admin_username: str,
    postgres_admin_password: str,
) -> dict[str, Any]:
    """
    Creates AiiDA and Airflow databases with a shared database user.

    This function:
    1. Creates a single PostgreSQL user (handling naming conflicts with suffixes)
    2. Creates the AiiDA database owned by this user
    3. Creates the Airflow database owned by the same user

    Both databases share the same user for simplicity and efficiency.

    Args:
        profile_name: The AiiDA profile name to use as the base
        postgres_hostname: PostgreSQL server hostname
        postgres_port: PostgreSQL server port
        postgres_admin_username: PostgreSQL admin username (must have CREATEDB and CREATEROLE privileges)
        postgres_admin_password: PostgreSQL admin password

    Returns:
        Dictionary containing database configuration for both AiiDA and Airflow:
        - database_*: AiiDA database configuration
        - airflow_database_*: Airflow database configuration
        Both configurations share the same username and password.

    Raises:
        ConnectionError: If unable to connect to PostgreSQL or create database/user
    """
    from aiida.manage.configuration.settings import AiiDAConfigDir

    # Connect to PostgreSQL using admin credentials
    psql_config = {
        'host': postgres_hostname,
        'port': postgres_port,
        'user': postgres_admin_username,
        'password': postgres_admin_password,
    }

    with psycopg.connect(**psql_config) as conn:
        # Set autocommit mode because CREATE DATABASE cannot run inside a transaction
        conn.autocommit = True

        # Create database user first (shared between AiiDA and Airflow)
        desired_username = f'aiida-{profile_name}'
        aiida_database_password = secrets.token_hex(15)
        aiida_database_username = PostgreSqlProfileCommands.create_user_from_username_template(conn, desired_username, aiida_database_password)


        if aiida_database_username != desired_username:
            logger.info(
                f"Database user '{desired_username}' already exists. "
                f"Created new user '{aiida_database_username}' instead."
            )

        # Create AiiDA database (using the shared user)
        desired_database_name = f'aiida-{profile_name}' 
        aiida_database_name = PostgreSqlProfileCommands.create_db_from_dbname_template(
            conn,
            desired_database_name,
            aiida_database_username,
        )

        if desired_database_name != aiida_database_name:
            logger.info(
                f"Database user '{desired_database_name}' already exists. "
                f"Created new user '{aiida_database_name}' instead."
            )
        
        # Create database user first (shared between airflow and Airflow)
        desired_username = f'airflow-{profile_name}'
        airflow_database_password = secrets.token_hex(15)
        airflow_database_username = PostgreSqlProfileCommands.create_user_from_username_template(conn, desired_username, airflow_database_password)

        if airflow_database_username != desired_username:
            logger.info(
                f"Database user '{desired_username}' already exists. "
                f"Created new user '{airflow_database_username}' instead."
            )

        # Create airflow database (using the shared user)
        desired_database_name = f'airflow-{profile_name}' 
        airflow_database_name = PostgreSqlProfileCommands.create_db_from_dbname_template(
            conn,
            desired_database_name,
            airflow_database_username,
        )

        if desired_database_name != airflow_database_name:
            logger.info(
                f"Database user '{desired_database_name}' already exists. "
                f"Created new user '{airflow_database_name}' instead."
            )

    aiida_config_folder = AiiDAConfigDir.get()
    return {'database_engine': 'postgresql_psycopg',
            'database_hostname': postgres_hostname,
            'database_port': postgres_port,
            'database_username': aiida_database_username,
            'database_password': aiida_database_password,
            'database_name': aiida_database_name,
            'repository_uri': Path(f'{aiida_config_folder / "repository" / profile_name}').as_uri(),
            'airflow_database_engine': 'postgresql_psycopg',
            'airflow_database_username': airflow_database_username,
            'airflow_database_password': airflow_database_password,
            'airflow_database_name': airflow_database_name,
        }


def delete_aiida_profile(profile_name: str, pg_admin_user: str, pg_admin_password: str):
    """
    Delete an AiiDA profile with Airflow support.

    Args:
        profile_name: Name of the profile to delete.
        pg_admin_user: PostgreSQL admin user (needed to drop databases and users).
                      If None, uses POSTGRES_USER env var or 'postgres'.
        pg_admin_password: PostgreSQL admin password.
                          If None, uses POSTGRES_PASSWORD env var or 'postgres'.
    """
    from aiida.manage.configuration import get_config
    from aiida import load_profile

    profile = load_profile(profile_name)
    assert profile.storage_config['database_engine'] == "postgresql_psycopg", "We only support PostgreSQL for the moment"

    verify_profile_supports_airflow(profile)
    delete_psql_database_from_aiida_profile(profile, pg_admin_user, pg_admin_password)
    get_config().delete_profile(profile.name, delete_storage=False)


def delete_psql_database_from_aiida_profile(profile: Profile, pg_admin_user: str, pg_admin_password: str):
    """
    Remove both AiiDA and Airflow databases and their associated users for a given profile.

    Uses admin credentials to connect to PostgreSQL and drop databases and users.

    Args:
        profile: AiiDA profile containing database configuration
        pg_admin_user: PostgreSQL admin user (needed to drop databases and users)
        pg_admin_password: PostgreSQL admin password
    """
    # Verify that the profile has Airflow support
    verify_profile_supports_airflow(profile)

    # Extract database information from profile storage config
    storage_config = profile.storage_config

    # AiiDA database configuration
    aiida_database_host = storage_config['database_hostname']
    aiida_database_port = storage_config['database_port']
    aiida_db_name = storage_config['database_name']
    aiida_db_username = storage_config['database_username']

    # Airflow database configuration
    airflow_db_name = storage_config['airflow_database_name']
    airflow_db_username = storage_config['airflow_database_username']

    logger.info(f"Removing databases and users for profile '{profile.name}'")
    logger.info(f"  Host: {aiida_database_host}:{aiida_database_port}")
    logger.info(f"  AiiDA database: {aiida_db_name}")
    logger.info(f"  Airflow database: {airflow_db_name}")
    logger.info(f"  Database users: {aiida_db_username}, {airflow_db_username}")

    # Connect to PostgreSQL using admin credentials
    # We use admin credentials because users cannot drop themselves
    psql_config = {
        'host': aiida_database_host,
        'port': aiida_database_port,
        'user': pg_admin_user,
        'password': pg_admin_password,
        'dbname': 'postgres'
    }

    with psycopg.connect(**psql_config) as conn:
        # Set autocommit mode because DROP DATABASE cannot run inside a transaction
        conn.autocommit = True

        # Drop AiiDA database
        logger.info(f"Dropping AiiDA database '{aiida_db_name}'")
        PostgreSqlProfileCommands.drop_db(conn, aiida_db_name)
        logger.info(f"Successfully dropped AiiDA database '{aiida_db_name}'")

        # Drop Airflow database
        logger.info(f"Dropping Airflow database '{airflow_db_name}'")
        PostgreSqlProfileCommands.drop_db(conn, airflow_db_name)
        logger.info(f"Successfully dropped Airflow database '{airflow_db_name}'")

        # Drop users (remove duplicates in case both databases share the same user)
        users_to_drop = list(set([aiida_db_username, airflow_db_username]))
        for username in users_to_drop:
            logger.info(f"Dropping database user '{username}'")
            PostgreSqlProfileCommands.drop_user(conn, username)
            logger.info(f"Successfully dropped database user '{username}'")

    logger.info(f"Successfully removed all databases and users for profile '{profile.name}'")

def create_aiida_profile(pg_host: str, pg_port: int, pg_admin_user: str, pg_admin_password: str, profile_name: str, overwrite: bool = True):
    """
    Set up Airflow database using the AiiDA profile's PostgreSQL server.

    Args:
        pg_host: PostgreSQL hostname (root credentials)
        pg_port: PostgreSQL port
        pg_admin_user: PostgreSQL admin user (used to create airflow database)
        pg_admin_password: PostgreSQL admin password
        profile_name: Profile name (used to name airflow database)
        overwrite: TODO
    """
    from aiida.manage.configuration import get_config
    from aiida.manage.configuration import create_default_user

    logger.info("Setting up AiiDA profile with Airflow support")
    logger.info(f"Profile name: {profile_name}")
    logger.info(f"PostgreSQL configuration:")
    logger.info(f"  Host: {pg_host}")
    logger.info(f"  Port: {pg_port}")
    logger.info(f"  Admin user: {pg_admin_user}")

    pg_database = f"aiida-{profile_name}"

    # Get AiiDA configuration (create if it doesn't exist)
    config = get_config(create=True)

    # Check if profile already exists
    if profile_name in config.profile_names:
        logger.info(f"Profile '{profile_name}' already exists")

        from airflow_provider_aiida.aiida_core import load_profile
        profile = load_profile(profile_name)
        if overwrite:
            logger.info(f"Deleting existing profile '{profile_name}'")
            delete_psql_database_from_aiida_profile(profile, pg_admin_user, pg_admin_password)
            config.remove_profile(profile_name)
            config.store()
            logger.info(f"Successfully deleted existing profile '{profile_name}'")
        else:
            logger.info(f"Using existing profile '{profile_name}'")
            return profile

    # Create profile configuration
    logger.info(f"Creating AiiDA and Airflow databases for profile '{profile_name}'")
    db_config = create_psql_database_from_aiida_profile(
        profile_name,
        pg_host,
        pg_port,
        pg_admin_user,
        pg_admin_password,
    )
    logger.info(f"Successfully created databases for profile '{profile_name}'")

    logger.info(f"Creating AiiDA profile '{profile_name}'")
    # creates profile in config and initializes database
    profile = create_profile(
        config,
        name=profile_name,
        storage_backend='core.psql_dos',
        storage_config=db_config,
        is_test_profile = True,
    )

    # Set as default profile and creates aiida user in database
    create_default_user(profile, f'{profile_name}@aiida.net')
    logger.info(f"Successfully created AiiDA profile '{profile_name}'")
    config.set_default_profile(profile.name, overwrite=True)
    config.store()
    logger.info(f"Set profile '{profile_name}' as default")

    # Initialize storage by loading the profile
    # This is necessary to create the database tables and initialize the storage backend
    logger.info("Initializing storage (creating database tables)")
    from airflow_provider_aiida.aiida_core import load_profile
    profile = load_profile(profile_name)
    logger.info("Successfully initialized storage and created database tables")

    # Show summary
    logger.info(f"Profile setup summary:")
    logger.info(f"  Profile name: {profile_name}")
    logger.info(f"  Database: {pg_host}:{pg_port}/{pg_database}")
    logger.info(f"  Storage backend: {profile.storage_backend}")
    logger.info(f"  Config directory: {config.dirpath}")
    logger.info(f"  Airflow home: {os.environ.get('AIRFLOW_HOME')}")

    import subprocess
    from aiida.manage.configuration import get_config

    logger.info("Initializing Airflow database schema")

    # Set up Airflow home directory in AiiDA config
    from_profile_create_airflow_config(profile)
    # Initialize Airflow database
    try:
        logger.info("Running Airflow database migrations")
        subprocess.run(
            ["airflow", "db", "migrate"],
            env=None,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("Successfully initialized Airflow database")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to initialize Airflow database: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise

    try:
        logger.info("Running Airflow DAG serialization")
        subprocess.run(
            ["airflow", "dags", "reserialize", "--bundle-name", "aiida_dags"],
            env=None,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("Successfully serialized DAGs")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to serialized DAGs: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise

    logger.info(f"Successfully completed setup for profile '{profile_name}'")


def from_profile_create_airflow_config(profile: Profile):
    """
    Create Airflow environment variables from an AiiDA profile.

    Args:
        profile: The AiiDA profile containing Airflow configuration

    Returns:
        Dictionary of environment variables for Airflow
    """
    from aiida.manage.configuration import get_config
    verify_profile_supports_airflow(profile)

    # Get Airflow home directory path
    config = get_config()

    # Extract Airflow database configuration from profile storage config
    storage_config = profile.storage_config

    airflow_db_user = storage_config["airflow_database_username"]
    airflow_db_password = storage_config["airflow_database_password"]
    airflow_db_host = storage_config["database_hostname"]
    airflow_db_port = storage_config["database_port"]
    airflow_db_name = storage_config["airflow_database_name"]

    # Build Airflow database connection string
    db_conn = f"postgresql+psycopg2://{airflow_db_user}:{airflow_db_password}@{airflow_db_host}:{airflow_db_port}/{airflow_db_name}"


    airflow_home = get_airflow_home(profile)
    airflow_home.mkdir(parents=True, exist_ok=True)

    dags_folder = airflow_home / "dags"
    dags_folder.mkdir(parents=False, exist_ok=True)
    dag_bundle = f'[{{"name":"aiida_dags","classpath":"airflow_provider_aiida.bundles.aiida_dag_bundle.AiidaDagBundle","kwargs":{{"output_dir": "{str(dags_folder)}"}}}}]'

    from airflow.configuration import AirflowConfigParser
    from base64 import b64encode
    num_workers = config.get_option("daemon.default_workers")

    airflow_config = AirflowConfigParser()
    airflow_config_file = airflow_home / 'airflow.cfg'

    # Read existing config if it exists to preserve secret keys
    if airflow_config_file.exists():
        airflow_config.read(airflow_config_file)

    # Generate secret keys for JWT authentication ONLY if they don't already exist
    # These keys must be the same across all Airflow services (scheduler, triggerer, api-server)
    fernet_key = b64encode(secrets.token_bytes(32)).decode('utf-8')
    jwt_secret = b64encode(secrets.token_bytes(16)).decode('utf-8')
    api_secret = b64encode(secrets.token_bytes(16)).decode('utf-8')

    airflow_config.set('core', 'parallelism', str(num_workers))
    airflow_config.set('core', 'dags_are_paused_at_creation', 'False')
    airflow_config.set('core', 'max_active_tasks_per_dag', '1000000')
    airflow_config.set('core', 'max_active_runs_per_dag', '1000000')
    airflow_config.set('core', 'default_timezone', 'system')
    airflow_config.set('core', 'load_examples', 'False')
    airflow_config.set('core', 'fernet_key', fernet_key)
    airflow_config.set('database', 'sql_alchemy_conn', db_conn)
    airflow_config.set('dag_processor', 'dag_bundle_config_list', dag_bundle)
    airflow_config.set('api_auth', 'jwt_secret', jwt_secret)
    airflow_config.set('api', 'secret_key', api_secret)
    airflow_config.set('api', 'host', "localhost")
    airflow_config.set('api', 'port', None)

    with open(airflow_config_file, 'w') as f:
        # Write with all documentation and comments
        airflow_config.write(
            f,
            include_descriptions=True,
            include_examples=True,
            include_env_vars=True,
            comment_out_everything=False,
            extra_spacing=True
        )
