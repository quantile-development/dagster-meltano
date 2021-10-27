FROM mcr.microsoft.com/vscode/devcontainers/python:3.9

ENV MELTANO_PROJECT_ROOT=/workspaces/dagster-meltano/meltano

COPY . /workspaces/dagster-meltano

RUN pip install -e /workspaces/dagster-meltano[development]