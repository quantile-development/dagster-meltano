# Dagster-meltano (Under development)
A dagster plugin that allows you to run Meltano pipelines using Dagster.

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/quantile-development/dagster-meltano)
[![Downloads](https://pepy.tech/badge/dagster-meltano/month)](https://pepy.tech/project/dagster-meltano)

## Installation
1. Install using pip `pip install dagster-meltano`.
2. Make sure you have an installed Meltano project.
3. Point the plugin to the root of the Meltano project by defining `MELTANO_PROJECT_ROOT`.

## Example
An example of a Dagster pipeline that runs a Meltano elt process.
```python
from dagster import pipeline
from dagster_meltano.solids import meltano_elt_solid

@pipeline
def meltano_pipeline():
    meltano_elt_solid(
        output_defs=[OutputDefinition(dagster_type=Nothing)],
        tap='tap-csv',
        target='target-jsonl',
        job_id='csv-to-jsonl' #Optional
    )
```

## Development
1. Open this repository in Visual Studio Code.
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin for Visual Studio Code.
3. Wait for the container setup, it should automatically install all Meltano plugins. 
4. Open the integrated terminal and start Dagit `dagit -f dagster/pipeline.py`
4. Visit `localhost:3000` to access Dagit.
