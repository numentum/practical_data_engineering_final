from dagster import repository

from pde_dagster.jobs.J1_process_pos_records import J1_process_pos_records
from pde_dagster.jobs.J2_partitioned_process_pos_records import J2_partitioned_process_pos_records


@repository
def pde_dagster():
    jobs = [J1_process_pos_records, J2_partitioned_process_pos_records]
    schedules = []
    sensors = []

    return jobs + schedules + sensors
