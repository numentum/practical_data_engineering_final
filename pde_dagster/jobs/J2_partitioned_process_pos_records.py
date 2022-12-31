from datetime import datetime
from dagster import job, monthly_partitioned_config

from pde_dagster.ops.J1_process_pos_records.extract_table_pos import extract_table_pos_op
from pde_dagster.ops.J1_process_pos_records.transform_record_pos import transform_record_pos_op
from pde_dagster.ops.load_table import load_table_op
from pde_dagster.resources.postgres import postgres_resource


@monthly_partitioned_config(start_date=datetime(2022, 1, 1))
def partitioned_config(start, _end):
    return {
        "ops": {
            "extract_table_pos_op": {
                "config": {"start_time": start.replace(tzinfo=None).strftime("%Y-%m-%d"), "end_time": _end.replace(tzinfo=None).strftime("%Y-%m-%d")}
            }
        }
    }


@job(config=partitioned_config, resource_defs={"postgres_resource": postgres_resource})
def J2_partitioned_process_pos_records():
    df = extract_table_pos_op()
    df = transform_record_pos_op(df)
    load_table_op(df)
