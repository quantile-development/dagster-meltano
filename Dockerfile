FROM mcr.microsoft.com/vscode/devcontainers/python:3.9

RUN mkdir /dagster

ENV MELTANO_PROJECT_ROOT=/workspaces/dagster-meltano/meltano
ENV DAGSTER_HOME=/dagster

COPY . /workspaces/dagster-meltano

RUN pip install -e /workspaces/dagster-meltano[development]