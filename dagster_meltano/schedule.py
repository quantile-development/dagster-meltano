from dagster import DefaultScheduleStatus, ScheduleDefinition

from dagster_meltano.utils import generate_dagster_name


class Schedule:
    def __init__(self, meltano_schedule: dict) -> None:
        self.name = meltano_schedule["name"]
        self.cron_interval = meltano_schedule["cron_interval"]
        self.job_name = meltano_schedule["job"]["name"]

    @property
    def dagster_name(self) -> str:
        return generate_dagster_name(self.name)

    @property
    def dagster_job_name(self) -> str:
        return generate_dagster_name(self.job_name)

    @property
    def dagster_schedule(self) -> ScheduleDefinition:
        return ScheduleDefinition(
            name=self.dagster_name,
            job_name=self.dagster_job_name,
            cron_schedule=self.cron_interval,
            default_status=DefaultScheduleStatus.RUNNING,
        )
