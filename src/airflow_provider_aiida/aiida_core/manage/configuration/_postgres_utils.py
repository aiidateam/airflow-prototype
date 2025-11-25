import psycopg
import logging

logger = logging.getLogger(__name__)

class PostgreSqlProfileCommands:
    """Static methods for PostgreSQL database and user management operations."""

    MAX_NAMING_ATTEMPTS = 100

    @staticmethod
    def db_exists(conn: 'psycopg.Connection', database_name: str) -> bool:
        """Check whether a postgres database with dbname exists.

        :param conn: PostgreSQL connection object
        :param database_name: Name of the database to check for
        :return: True if database exists, False otherwise
        """
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT datname FROM pg_database WHERE datname = %s",
                (database_name,)
            )
            output = cursor.fetchone()
        return bool(output) 

    @staticmethod
    def user_exists(conn: 'psycopg.Connection', database_username: str) -> bool:
        """Check whether a postgres user exists.

        :param conn: PostgreSQL connection object
        :param database_username: Name of the user to check for
        :return: True if user exists, False otherwise
        """
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT usename FROM pg_user WHERE usename = %s",
                (database_username,)
            )
            output = cursor.fetchone()
        return bool(output)


    @staticmethod
    def create_db(conn: 'psycopg.Connection', database_name: str, database_owner: str) -> None:
        """Create a PostgreSQL database with the specified owner.

        Note: CREATE DATABASE cannot run inside a transaction, so the connection
        should be in autocommit mode.

        :param conn: PostgreSQL connection object (should be in autocommit mode)
        :param database_name: Name of the database to create
        :param database_owner: Username of the database owner
        """
        with conn.cursor() as cursor:
            # Use SQL identifiers properly to avoid SQL injection
            cursor.execute(
                psycopg.sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    psycopg.sql.Identifier(database_name),
                    psycopg.sql.Identifier(database_owner)
                )
            )

    @staticmethod
    def create_user(conn: 'psycopg.Connection', username: str, password: str) -> None:
        """Create a PostgreSQL user with the specified password.

        :param conn: PostgreSQL connection object
        :param username: Username to create
        :param password: Password for the user
        """
        with conn.cursor() as cursor:
            # Use SQL identifiers and literals properly to avoid SQL injection
            cursor.execute(
                psycopg.sql.SQL("CREATE USER {} WITH PASSWORD {}").format(
                    psycopg.sql.Identifier(username),
                    psycopg.sql.Literal(password)
                )
            )

    @staticmethod
    def create_db_from_dbname_template(conn: 'psycopg.Connection', dbname_template: str, dbowner: str) -> str:
        """
        Safely create a PostgreSQL database, handling naming conflicts.

        If a database with the given name already exists, this function will append
        a numeric suffix (-1, -2, etc.) to create a unique database name.

        Note: CREATE DATABASE cannot run inside a transaction in PostgreSQL, so the
        connection should be in autocommit mode.

        Args:
            conn: PostgreSQL connection object (should be in autocommit mode)
            dbname_template: Desired database name template
            dbowner: Username of the database owner (must already exist)

        Returns:
            str: The actual database name that was created (may differ from desired if conflicts exist)

        Raises:
            RuntimeError: If unable to find a unique database name after MAX_NAMING_ATTEMPTS tries
        """
        actual_dbname = dbname_template
        counter = 1

        for _ in range(PostgreSqlProfileCommands.MAX_NAMING_ATTEMPTS):
            # Try to create the database
            db_exists = PostgreSqlProfileCommands.db_exists(conn, actual_dbname)
            if db_exists:
                actual_dbname = f"{dbname_template}-{counter}"
                counter += 1
            else:
                break
        else:
            raise RuntimeError(
                f"Could not find a unique database name after {PostgreSqlProfileCommands.MAX_NAMING_ATTEMPTS} attempts. "
                f"Last tried: {actual_dbname}"
            )

        PostgreSqlProfileCommands.create_db(conn, actual_dbname, dbowner)
        logger.info(f"Created PostgreSQL database '{actual_dbname}' with owner '{dbowner}'")

        return actual_dbname

    @staticmethod
    def create_user_from_username_template(conn: 'psycopg.Connection', username_template: str, password: str) -> str:
        """
        Safely create a PostgreSQL user, handling naming conflicts.

        If a user with the given username already exists, this function will append
        a numeric suffix (_1, _2, etc.) to create a unique username.

        Args:
            conn: PostgreSQL connection object
            username_template: Desired username template
            password: Password for the user

        Returns:
            str: The actual username that was created (may differ from desired if conflicts exist)

        Raises:
            RuntimeError: If unable to find a unique username after MAX_NAMING_ATTEMPTS tries
        """
        actual_username = username_template
        counter = 1

        for _ in range(PostgreSqlProfileCommands.MAX_NAMING_ATTEMPTS):
            # Check if user exists
            user_exists = PostgreSqlProfileCommands.user_exists(conn, actual_username)
            if user_exists:
                actual_username = f"{username_template}_{counter}"
                counter += 1
            else:
                break
        else:
            raise RuntimeError(
                f"Could not find a unique username after {PostgreSqlProfileCommands.MAX_NAMING_ATTEMPTS} attempts. "
                f"Last tried: {actual_username}"
            )

        PostgreSqlProfileCommands.create_user(conn, actual_username, password)
        logger.info(f"Created PostgreSQL user '{actual_username}'")

        return actual_username

    @staticmethod
    def drop_db(conn: 'psycopg.Connection', database_name: str) -> None:
        """Drop a PostgreSQL database.

        This method will terminate all active connections to the database before dropping it.

        Note: DROP DATABASE cannot run inside a transaction, so the connection
        should be in autocommit mode.

        :param conn: PostgreSQL connection object (should be in autocommit mode)
        :param database_name: Name of the database to drop
        """
        with conn.cursor() as cursor:
            # Terminate existing connections to the database
            cursor.execute(
                psycopg.sql.SQL("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = {}
                    AND pid <> pg_backend_pid()
                """).format(psycopg.sql.Literal(database_name))
            )
            # Drop the database
            cursor.execute(
                psycopg.sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    psycopg.sql.Identifier(database_name)
                )
            )

    @staticmethod
    def drop_user(conn: 'psycopg.Connection', username: str) -> None:
        """Drop a PostgreSQL user.

        :param conn: PostgreSQL connection object
        :param username: Name of the user to drop
        """
        with conn.cursor() as cursor:
            cursor.execute(
                psycopg.sql.SQL("DROP USER IF EXISTS {}").format(
                    psycopg.sql.Identifier(username)
                )
            )
