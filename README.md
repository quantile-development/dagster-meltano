# Dagster-meltano

A dagster plugin that allows you to run Meltano using Dagster.

[![Downloads](https://pepy.tech/badge/dagster-meltano/month)](https://pepy.tech/project/dagster-meltano)

## Installation

You can install using `pip install dagster-meltano`.

## Examples

An example of automatically loading all jobs and schedules from your Meltano project.

```python
from dagster import repository
from dagster_meltano import load_jobs_from_meltano_project

meltano_jobs = load_jobs_from_meltano_project("<path-to-meltano-root>")

@repository
def repository():
    return [meltano_jobs]
```

An example of running a abitrary `meltano run` command.

```python
from dagster import repository, job
from dagster_meltano import meltano_resource, meltano_run_op

@job(resource_defs={"meltano": meltano_resource})
def meltano_run_job():
    tap_done = meltano_run_op("tap-1 target-1")()
    meltano_run_op("tap-2 target-2")(tap_done)

@repository()
def repository():
    return [meltano_run_job]
```

## Development using VSCode

1. Open this repository in Visual Studio Code.
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin for Visual Studio Code.
3. Go to the example Meltano project root `cd meltano_project`
4. Install all plugins `meltano install`
5. Start dagit `meltano invoke dagster:start`
6. Visit `localhost:3000` to access Dagit.
