from dagster import job

from pde_dagster.ops.extract_table_pos import extract_table_pos_op
from pde_dagster.resources.postgres import postgres_resource


@job(resource_defs={"postgres_resource": postgres_resource})
def J1_process_pos_records():
    extract_table_pos_op()
