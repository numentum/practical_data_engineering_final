from datetime import datetime
from sqlalchemy import MetaData, Table
from dagster import sensor, DefaultSensorStatus, build_resources, RunRequest
from pde_dagster.jobs.J3_partitioned_process_records import J3_partitioned_process_records
from pde_dagster.resources.postgres import postgres_resource


def get_results_count_in_range(engine, table_name, filter_col, start_time):
    with engine.connect() as connection:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload=True, autoload_with=engine)

        assert filter_col in table.columns, f"Provided filter column '{filter_col}' does not exist in table '{table_name}'."

        query_str = f"SELECT COUNT(*), MAX({filter_col}) from {table_name} WHERE '{start_time}' < created_at"

        result = connection.execute(query_str).mappings().all()

        return result[0]["count"], result[0]["max"].isoformat() if result[0]["max"] else None


@sensor(job=J3_partitioned_process_records, default_status=DefaultSensorStatus.STOPPED, minimum_interval_seconds=5)
def pos_transaction_sensor(context):
    with build_resources({"postgres_resource": postgres_resource}) as resource:
        engine, _, _ = resource.postgres_resource

        start_timestamp = context.cursor if context.cursor is not None else "2000-01-01 00:00:00.000000"

        results_count, end_timestamp = get_results_count_in_range(engine, "pos_transactions", "created_at", start_timestamp)

        if results_count > 0:
            if end_timestamp is not None:
                context.update_cursor(str(end_timestamp))

            run_config = {
                "ops": {"extract_table_op": {"config": {"table_name": "pos_transactions", "start_time": start_timestamp, "end_time": end_timestamp}}},
                "resources": {"job_config": {"config": {"table_name": "pos_transactions"}}},
            }

            yield RunRequest(run_key=str(datetime.now()), run_config=run_config)
