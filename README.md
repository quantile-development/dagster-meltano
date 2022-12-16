# Dagster-meltano (Archived)

## This repository is no longer in use. If you want to use Meltano inside Dagster, check out the [dagster-ext](https://github.com/quantile-development/dagster-ext) repository.

A dagster plugin that allows you to run Meltano pipelines using Dagster.

[![Downloads](https://pepy.tech/badge/dagster-meltano/month)](https://pepy.tech/project/dagster-meltano)

## Installation
1. Install using pip `pip install dagster-meltano`.
2. Make sure you have an installed Meltano project.
3. Point the plugin to the root of the Meltano project by defining `MELTANO_PROJECT_ROOT`.

## Example
An example of a Dagster pipeline that runs a Meltano elt process.

```python

from dagster import OutputDefinition, Nothing
from dagster_meltano.tests import pipeline
from dagster_meltano.solids import meltano_elt_solid


@pipeline
def meltano_pipeline():
    meltano_elt_solid(
        output_defs=[OutputDefinition(dagster_type=Nothing)],
        tap='tap-csv',
        target='target-jsonl',
        job_id='csv-to-jsonl'  # Optional
    )
```

## Development
### Setup using VSCode
1. Open this repository in Visual Studio Code.
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin for Visual Studio Code.
3. Wait for the container setup, it should automatically install all Meltano plugins. 
4. Open the integrated terminal and start Dagit `dagit -f dagster_meltano/tests/pipeline.py`
4. Visit `localhost:3000` to access Dagit.

### Setup using other IDEs
1. Create a virtual environment
2. Pip install dependencies: `pip install dagster meltano`
3. Install Meltano plugins: `cd meltano && meltano install && cd ..`
4. Set env vars: `export MELTANO_PROJECT_ROOT=<path/to/meltano>`
5. Run dagit: `dagit -f dagster_meltano/tests/pipeline.py`

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
