# Dagster-meltano
A dagster plugin that allows you to run integrate your Meltano project inside Dagster.

[![Downloads](https://pepy.tech/badge/dagster-meltano/month)](https://pepy.tech/project/dagster-meltano)

## Development
### Setup using VSCode
1. Open this repository in Visual Studio Code.
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin for Visual Studio Code.
3. ...

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