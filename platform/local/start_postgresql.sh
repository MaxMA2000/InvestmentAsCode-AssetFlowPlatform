sudo rm /tmp/.s.PGSQL.5432.lock

sudo rm /tmp/.s.PGSQL.5432

export PGDATA="/Users/maxma/Library/Application Support/Postgres/InvestmentAsCode/"

postgres

# localhost:5432


# https://www.postgresql.org/download/macosx/

# check port usage: `sudo lsof -i :5432`
# kill port application: `sudo kill -9 99213`

# - To Set Up the PostgreSQL Database, do the following steps:
#   1. create database "AssetFlowPlatform"
#   2. Open Query Tools, run the following SQL Command:
#   3. check in `setup_postgres.sql``
