# Runinng tests

## Testing environment

The test environment (`hatch-test`) is configured in `pyproject.toml` with:
- PostgreSQL connection on port 5434
- Airflow home directory in `.pytest/airflow`
- Custom DAG bundle configuration
- Test dependencies (pytest, psycopg2-binary, asyncpg)

## Start PostgreSQL service

The test environment uses PostgreSQL on port 5434. Start the PostgreSQL service:

```bash
docker compose -f docker-compose.test.yml up -d
```

Check if the service is running:

```bash
docker ps
```

Expected output:
```
CONTAINER ID   IMAGE         COMMAND                  CREATED      STATUS                PORTS                                         NAMES
6de79ae7f116   postgres:14   "docker-entrypoint.s…"   2 days ago   Up 2 days (healthy)   0.0.0.0:5434->5432/tcp, [::]:5434->5432/tcp   airflow-provider-aiida-postgres-1
```

## Run unit tests

For regular unit tests that don't require Airflow services:

```bash
hatch test
```

This runs all non-integration tests using pytest.
To clean test artifacts including airflow.cfg and logs.
```bash
hatch run hatch-test.py3.11:clean
```

## Integration tests

Integration tests require running Airflow services (scheduler, triggerer, dag-processor). These tests are marked with `@pytest.mark.integration`.


### Start airflow services

In separate terminal windows/tabs, start each service:

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

```bash
hatch run hatch-test.py3.11:integration
```

Or use hatch test with pytest args:

```bash
hatch test -- -m integration
```
