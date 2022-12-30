import os

from dagster import repository

from dagster_meltano import load_jobs_from_meltano_project

MELTANO_PROJECT_DIR = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())
MELTANO_BIN = os.getenv("MELTANO_BIN", "meltano")


@repository
def meltano_jobs():
    return [load_jobs_from_meltano_project(MELTANO_PROJECT_DIR)]
