#!/bin/bash

set -e
set -u

function create_user_and_database() {
    local database=$1
    local username=$2
    local password=$3
    echo "Creating user '$username' and database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $username WITH ENCRYPTED PASSWORD '$password';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $username;
EOSQL
    echo "  User '$username' and database '$database' created successfully"
}

# Metadata database
if [ -z "${METADATA_DATABASE_NAME}" ] || [ -z "${METADATA_DATABASE_USERNAME}" ] || [ -z "${METADATA_DATABASE_PASSWORD}" ]; then
    echo "ERROR: Missing metadata database environment variables"
    exit 1
fi
create_user_and_database "$METADATA_DATABASE_NAME" "$METADATA_DATABASE_USERNAME" "$METADATA_DATABASE_PASSWORD"

# Celery result backend database
if [ -z "${CELERY_BACKEND_NAME}" ] || [ -z "${CELERY_BACKEND_USERNAME}" ] || [ -z "${CELERY_BACKEND_PASSWORD}" ]; then
    echo "ERROR: Missing celery database environment variables"
    exit 1
fi
create_user_and_database "$CELERY_BACKEND_NAME" "$CELERY_BACKEND_USERNAME" "$CELERY_BACKEND_PASSWORD"

# ELT database
if [ -z "${ELT_DATABASE_NAME}" ] || [ -z "${ELT_DATABASE_USERNAME}" ] || [ -z "${ELT_DATABASE_PASSWORD}" ]; then
    echo "ERROR: Missing ELT database environment variables"
    exit 1
fi
create_user_and_database "$ELT_DATABASE_NAME" "$ELT_DATABASE_USERNAME" "$ELT_DATABASE_PASSWORD"

echo "All databases and users created successfully"
