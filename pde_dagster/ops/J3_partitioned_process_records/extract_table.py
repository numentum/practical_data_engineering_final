from dagster import op

from practical_data_engineering_shared.io.extract import extract_table


@op(name="extract_table_op", required_resource_keys={"postgres_resource", "job_config"})
def extract_table_op(context):
    _, _, pg_conn = context.resources.postgres_resource

    table_name = context.resources.job_config["table_name"]

    start_time = context.op_config.get("start_time")
    end_time = context.op_config.get("end_time")

    df = extract_table(table_name, pg_conn=pg_conn, start_time=start_time, end_time=end_time)

    context.log.info(f"Extracted {len(df)} records from {table_name}")

    return df
