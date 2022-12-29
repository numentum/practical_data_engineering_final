from dagster import ScheduleDefinition
from pde_dagster.jobs.J1_process_pos_records import J1_process_pos_records

process_records_schedule = ScheduleDefinition(job=J1_process_pos_records, cron_schedule="0 * * * *")
