import os
from dagster import resource
from contextlib import contextmanager
from sqlalchemy import create_engine


@resource
@contextmanager
def postgres_resource(context):
    pg_connection_string = os.getenv("PG_CONN")

    assert pg_connection_string is not None, "PG_CONN environment variable not set"

    engine = create_engine(pg_connection_string)
    connection = engine.connect()

    try:
        yield [engine, connection, pg_connection_string]
    finally:
        connection.close()
        engine.dispose()
