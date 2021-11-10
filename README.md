# Dagster-meltano (Under development)
A dagster plugin that allows you to run Meltano pipelines using Dagster.

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/quantile-development/dagster-meltano)
[![Downloads](https://pepy.tech/badge/dagster-meltano/month)](https://pepy.tech/project/dagster-meltano)

## Installation
1. Install using pip `pip install dagster-meltano`.
2. Make sure you have an installed Meltano project.
3. Point the plugin to the root of the Meltano project by defining `MELTANO_PROJECT_ROOT`.

## Examples
An example of a Dagster pipeline that runs a Meltano elt process.

```python
import json
from dagster import OutputDefinition, Nothing, pipeline
from dagster_meltano.solids import MeltanoEltSolid


@pipeline
def meltano_pipeline():
    MeltanoEltSolid(
        name="tap_csv_target_jsonl",
        tap="tap-csv",
        target="target-jsonl",
        job_id="csv-to-jsonl",
        tap_config={},
        target_config={
            "destination_path": "load"
        },
        env_vars={"TAP_CSV__SELECT": json.dumps(["sample.id"])},
        select_patterns=[["sample", "first_name"], ["--list", "--all"]]
    ).solid()
```

You can also inject information from previous solids during runtime.

```python
import json
from dagster import pipeline, solid
from dagster_meltano.solids import MeltanoEltSolid
from dagster_meltano.dagster_types import (
    MeltanoEltArgsType,
    MeltanoEnvVarsType,
    MeltanoSelectPatternsType,
)

@solid
def elt_args() -> MeltanoEltArgsType:
    return {
        "tap": "tap-csv",
        "target": "target-jsonl",
        "job_id": "csv-to-jsonl",
    }

@solid
def env_vars() -> MeltanoEnvVarsType:
    return {"TAP_CSV__SELECT": json.dumps(["sample.id"])}

@solid
def select_patterns() -> MeltanoSelectPatternsType:
    return [["sample", "*"], ['--list', '--all']]

@pipeline
def meltano_pipeline():
    MeltanoEltSolid(
        name="tap_csv_target_jsonl",
        tap_config={},
        target_config={
            "destination_path": "load"
        }
    ).solid(
        elt_args=elt_args(), 
        env_vars=env_vars(),
        select_patterns=select_patterns(),
    )
```

The last way to configure the Meltano elt solid is to overwrite the Dagster configuration. 

![configuration overwrite](./images/meltano-elt-solid-config.jpg)

## Development
### Setup using VSCode
1. Open this repository in Visual Studio Code.
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin for Visual Studio Code.
3. Wait for the container setup, it should automatically install all Meltano plugins. 
4. Open the integrated terminal and start Dagit `dagit -f dagster_meltano/tests/repo.py`
4. Visit `localhost:3000` to access Dagit.

### Setup using other IDEs
1. Create a virtual environment
2. Pip install dependencies: `pip install dagster meltano`
3. Install Meltano plugins: `cd meltano && meltano install && cd ..`
4. Set env vars: `export MELTANO_PROJECT_ROOT=<path/to/meltano>`
5. Run dagit: `dagit -f dagster_meltano/tests/repo.py`

## Testing and Linting
We use [Dagster's default setup](https://docs.dagster.io/community/contributing#developing-dagster) 
for testing and linting.

### Linting
Specifically linting can be accomplished by installing the appropriate linters:

```shell
pip install black pylint isort
```

And then running them against the codebase:

```shell
black dagster_meltano/ && isort dagster_meltano/ && pylint dagster_meltano/ 
```