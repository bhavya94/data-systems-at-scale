# data-systems-at-scale

Data pipelines and distributed processing, built up one concept per pull request. Each concept is a small
command you can run, with a test. Its output shows what happens to the data, step by step. You see which files a
job read, what it wrote, and what changed.

## Run it

Needs [uv](https://docs.astral.sh/uv/). It installs Python 3.14 if you don't have it.

```console
uv sync
uv run pytest
uv run dsas --help
```

## Layout

```
src/data_systems_at_scale/
  cli.py        the dsas command; each concept adds a subcommand
  pipelines/    jobs that turn raw data into outputs, and how to fix the outputs
tests/          one behavior test per concept
```
