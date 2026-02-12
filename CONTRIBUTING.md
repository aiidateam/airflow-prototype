# Run tests

## Quick start

To run only unit tests that do not require and Airflow services to be started.

```bash
# 1. Start PostgreSQL with docker compose
hatch run hatch-test.py3.11:start-psql-service

# 2. Create AiiDA profile and databases
hatch run hatch-test.py3.11:setup-profile

# 4. Run unit tests
hatch run hatch-test.py3.11:unit-tests
```

## Unit tests

### Setup test environment

The test environment (`hatch-test`) is configured in `pyproject.toml` with:
- PostgreSQL connection on port 5434
- AiiDA configuration in `.pytest/.aiida/`
- AiiDA profile named `test`
- Airflow home directory in `.pytest/.aiida/test/airflow/`
- Custom DAG bundle configuration
- Test dependencies (pytest, psycopg2-binary, asyncpg)

The `hatch-test` environment automatically sets these required environment variables.
Please check the environment variables in the `hatch-test` environment and how the scripts use them if you want to adapt them to your own.
**Important:** Tests and setup scripts **require** these environment variables. They are automatically set when using the scripts `hatch run hatch-test.py3.11:*` commands.
If you want to run tests manually outside of hatch, you must set them yourself.

#### 1. Start PostgreSQL service

The test environment uses PostgreSQL. Start the PostgreSQL service:

```bash
hatch run hatch-test.py3.11:start-psql-service
```

This will start PostgreSQL on port 5434 with the admin user `postgres` (password: `postgres`).

Check if the service is running:

```bash
hatch run hatch-test.py3.11:status-psql-service
```

Expected output:
```
CONTAINER ID   IMAGE         COMMAND                  CREATED      STATUS                PORTS                                         NAMES
6de79ae7f116   postgres:14   "docker-entrypoint.s…"   2 days ago   Up 2 days (healthy)   0.0.0.0:5434->5432/tcp, [::]:5434->5432/tcp   airflow-provider-aiida-postgres-1
```

#### 2. Setup AiiDA test profile

Create the AiiDA test profile that uses the PostgreSQL on port 5434:

```bash
hatch run hatch-test.py3.11:setup-profile
```

This script will:
1. Create a PostgreSQL user `aiida-test` with password `password`
2. Create two databases owned by `aiida-test`:
   - `aiida-test`: AiiDA database
   - `airflow-test`: Airflow database
3. Create an AiiDA profile named `test` using the `aiida-test` database
4. Initialize the AiiDA storage (create tables)
5. Set the profile as the default AiiDA profile
6. Initialize the Airflow database (run migrations)
7. Set up the Airflow home directory structure

**Database Hierarchy:**
```
postgres (admin)
  └── aiida-test (user created by setup-profile)
       ├── aiida-test (database for AiiDA)
       └── airflow-test (database for Airflow)
```

The profile will be located at `.pytest/.aiida/test/` (or `$AIIDA_PATH/.aiida/test/` if `AIIDA_PATH` is set) with the following structure:
```
.pytest/.aiida/test/
└── airflow/                    # Airflow files
    ├── dags/                   # DAG files
    └── airflow.cfg             # Airflow configuration
```

**Note:** If the profile already exists, the script will recreate it. If you choose to recreate, it will:
- Remove the existing AiiDA profile configuration
- Drop both aiida and airflow databases
- Drop the aiida `test` user
- Recreate everything from scratch

## Run unit test

For regular unit tests that don't require Airflow services:

```bash
hatch run hatch-test.py3.11:unit-tests
```

Or use hatch test with pytest args:

```bash
hatch test -- -m 'not integration'
```

## Run integration tests

Integration tests require running Airflow services (scheduler, triggerer, dag-processor). These tests are marked with `@pytest.mark.integration`.

### Start Airflow services in foreground (recommended for debugging)

If you prefer to start services manually in separate terminals:

```bash
# Terminal 1: Scheduler
hatch run hatch-test.py3.11:scheduler

# Terminal 2: Triggerer
hatch run hatch-test.py3.11:triggerer

# Terminal 3: DAG Processor
hatch run hatch-test.py3.11:dag-processor

# Terminal 4: API Server
hatch run hatch-test.py3.11:api-server
```

### Run integration tests

Once services are running, run the integration tests:

```bash
hatch run hatch-test.py3.11:integration
```

Or use hatch test with pytest args:

```bash
hatch test -- -m integration
```

##  Run all tests

After setup you can run all tests using

```bash
hatch test
```

## Clean test artifacts

Be sure that all airflow services and the docker service have been stopped.
The docker service can be stopped with.
```
hatch run hatch-test.py3.11:stop-psql-service
```

To clean test artifacts including the PostgreSQL databasese cluster, as well as the aiida and the airflow config.
```bash
hatch run hatch-test.py3.11:clean
```
