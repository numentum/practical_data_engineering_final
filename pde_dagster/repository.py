from dagster import repository

from pde_dagster.jobs.J1_process_pos_records import J1_process_pos_records
from pde_dagster.jobs.J2_partitioned_process_pos_records import J2_partitioned_process_pos_records
from pde_dagster.jobs.J3_partitioned_process_records import J3_partitioned_process_records
from pde_dagster.jobs.J4_streamed_process_records import J4_streamed_process_records
from pde_dagster.jobs.J5_process_files import J5_process_files
from pde_dagster.schedules.process_records_schedule import process_records_schedule
from pde_dagster.sensors.pos_transaction_sensor import pos_transaction_sensor


@repository
def pde_dagster():
    jobs = [J1_process_pos_records, J2_partitioned_process_pos_records, J3_partitioned_process_records, J4_streamed_process_records, J5_process_files]
    schedules = [process_records_schedule]
    sensors = [pos_transaction_sensor]

    return jobs + schedules + sensors
