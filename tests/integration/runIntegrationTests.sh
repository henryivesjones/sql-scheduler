#!/bin/bash
set -e
if [ -n "$GITHUB_ACTION" ]; then
    cd ../../
    python3 -m build >>/dev/null
    pip3 install .
else
    cd "$(dirname "${BASH_SOURCE[0]}")"
    cd ../../

    echo "Building sql-scheduler from source..."
    rm -rf dist
    python3 -m build >>/dev/null

    cd "$(dirname "${BASH_SOURCE[0]}")"

    rm -rf .venv
    python3 -m venv .venv
    source .venv/bin/activate
    echo "Installing sql-scheduler in test virtual environment..."
    cd ../../
    pip3 install . >>/dev/null

fi
cd tests/integration

export INTEGRATION_PG_PORT=9876
export SQL_SCHEDULER_DEV_SCHEMA="dev"
export SQL_SCHEDULER_CACHE_DURATION="-1"
export SQL_SCHEDULER_INSERT_DIRECTORY="$(dirname "${BASH_SOURCE[0]}")/sql/insert/"
export SQL_SCHEDULER_DDL_DIRECTORY="$(dirname "${BASH_SOURCE[0]}")/sql/ddl/"
export SQL_SCHEDULER_DSN="postgres://postgres:postgres@localhost:$INTEGRATION_PG_PORT/postgres"
docker run --name sql-scheduler-integration-pg -e POSTGRES_PASSWORD=postgres -p $INTEGRATION_PG_PORT:5432 -d postgres:13 >>/dev/null

python3 setup.py
python3 integration.py || EXIT_CODE=$?

docker stop sql-scheduler-integration-pg >>/dev/null
docker rm sql-scheduler-integration-pg >>/dev/null

rm -rf .venv

exit $EXIT_CODE
