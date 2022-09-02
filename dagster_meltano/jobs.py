import os
from functools import lru_cache
from typing import List, Optional

from dagster import DefaultScheduleStatus, JobDefinition, ScheduleDefinition, job, op


@lru_cache
def load_jobs_from_meltano_project(
    project_dir: Optional[str],
) -> List[JobDefinition]:
    @op
    def run_tap_csv_to_target_postgres():
        os.system('meltano run tap-csv-to-target-postgres')

    @job
    def tap_csv_to_target_postgres():
        run_tap_csv_to_target_postgres()

    tap_csv_to_target_postgres_schedule = ScheduleDefinition(
        job=tap_csv_to_target_postgres,
        cron_schedule="@hourly",
        default_status=DefaultScheduleStatus.RUNNING,
    )

    return [tap_csv_to_target_postgres, tap_csv_to_target_postgres_schedule]
