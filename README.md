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

An example of running an abitrary `meltano run` command.

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

You can inject Meltano config with the following Dagster config.

```yaml
ops:
  tap_smoke_test_target_jsonl:
    config:
      env:
        TAP_SMOKE_TEST_STREAMS: '[{"stream_name": "new-stream", "input_filename": "demo.json"}]'
```

An example of running an arbitrary Meltano command.

```python
from dagster import repository, job
from dagster_meltano import meltano_resource, meltano_command_op

@job(resource_defs={"meltano": meltano_resource})
def meltano_command_job():
    meltano_command_op("install loader tap-smoke-test")()

@repository()
def repository():
    return [meltano_command_job]
```

## Development using VSCode

1. Open this repository in Visual Studio Code.
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin for Visual Studio Code.
3. Go to the example Meltano project root `cd meltano_project`
4. Install all plugins `meltano install`
5. Start dagit `meltano invoke dagster:start`
6. Visit `localhost:3000` to access Dagit.
