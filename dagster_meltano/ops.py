import os

from dagster import OpDefinition, op


@op
def meltano_run_op():
    return None


def meltano_install_op(command: str = "") -> OpDefinition:
    @op
    def meltano_install():
        os.system(f'meltano install {command}')

    return meltano_install
