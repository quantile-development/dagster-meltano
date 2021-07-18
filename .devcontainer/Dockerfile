FROM mcr.microsoft.com/vscode/devcontainers/python:3.8

ENV MELTANO_PROJECT_ROOT=/workspaces/dagster-meltano/meltano

RUN pipx install meltano==1.78.0
RUN pipx install dagit==0.12.2

RUN mkdir -p /workspaces/dagster-meltano/dagster_meltano
RUN ln -s /workspaces/dagster-meltano/dagster_meltano /usr/local/py-utils/venvs/dagit/lib/python3.8/site-packages/dagster_meltano

CMD dagit -f /workspaces/dagster-meltano/dagster/pipeline.py