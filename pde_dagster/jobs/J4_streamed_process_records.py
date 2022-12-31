from dagster import job, in_process_executor
from pde_dagster.resources.postgres import postgres_resource
from pde_dagster.ops.J4_streamed_process_records.streamed_process_records import streamed_process_records_op


@job(
    config={"ops": {"streamed_process_records_op": {"config": {"query_str": "SELECT * FROM pos_transactions"}}}},
    resource_defs={"postgres_resource": postgres_resource},
    executor_def=in_process_executor,
)
def J4_streamed_process_records():
    streamed_process_records_op()
