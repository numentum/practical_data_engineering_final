from datetime import datetime
from dagster import job, monthly_partitioned_config, make_values_resource, in_process_executor

from pde_dagster.ops.J3_partitioned_process_records.extract_table import extract_table_op
from pde_dagster.ops.J3_partitioned_process_records.transform_record import transform_record_op
from pde_dagster.ops.load_table import load_table_op
from pde_dagster.resources.postgres import postgres_resource


@monthly_partitioned_config(start_date=datetime(2020, 1, 1))
def partitioned_config(start, _end):
    return {
        "ops": {
            "extract_table_op": {
                "config": {"start_time": start.replace(tzinfo=None).strftime("%Y-%m-%d"), "end_time": _end.replace(tzinfo=None).strftime("%Y-%m-%d")}
            }
        },
        "resources": {"job_config": {"config": {"table_name": "pos_transactions"}}},
    }


@job(
    config=partitioned_config,
    resource_defs={"postgres_resource": postgres_resource, "job_config": make_values_resource(table_name=str)},
    executor_def=in_process_executor,
)
def J3_partitioned_process_records():
    df = extract_table_op()
    df = transform_record_op(df)
    load_table_op(df)
