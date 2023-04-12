#!/bin/bash
set -e
echo "Building and installing sql-scheduler in test virtual environment..."
if [ -n "$GITHUB_ACTION" ]; then
    cd ../../
    pip3 install . >>/dev/null
else
    cd "$(dirname "${BASH_SOURCE[0]}")"
    pwd
    rm -rf .venv
    python3 -m venv .venv
    source .venv/bin/activate
    cd ../../
    pwd
    pip3 install . >>/dev/null

fi
cd tests/integration
INTEGRATION_DIR=$(pwd)

export INTEGRATION_PG_PORT=9876
export SQL_SCHEDULER_DEV_SCHEMA="dev"
export SQL_SCHEDULER_CACHE_DURATION="-1"
export SQL_SCHEDULER_INSERT_DIRECTORY="${INTEGRATION_DIR}/sql/insert/"
export SQL_SCHEDULER_DDL_DIRECTORY="${INTEGRATION_DIR}/sql/ddl/"
export SQL_SCHEDULER_DSN="postgres://postgres:postgres@localhost:$INTEGRATION_PG_PORT/postgres"
docker run --name sql-scheduler-integration-pg -e POSTGRES_PASSWORD=postgres -p $INTEGRATION_PG_PORT:5432 -d postgres:13 >>/dev/null

pwd
python3 setup.py
python3 integration.py || EXIT_CODE=$?

docker stop sql-scheduler-integration-pg >>/dev/null
docker rm sql-scheduler-integration-pg >>/dev/null

rm -rf .venv

exit $EXIT_CODE
