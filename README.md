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

## Raw data: written once, one folder per day

`dsas raw` writes made-up orders, 200 a day for 7 days, into `lake/raw/orders/`. Each day gets its own folder,
and each order is one line of JSON. The same seed always gives the same orders.

```console
$ uv run dsas raw
    7.5ms  MainThread  wrote raw/orders/date=2026-10-01/part-0.jsonl: 200 orders
    9.0ms  MainThread  wrote raw/orders/date=2026-10-02/part-0.jsonl: 200 orders
   10.4ms  MainThread  wrote raw/orders/date=2026-10-03/part-0.jsonl: 200 orders
   11.8ms  MainThread  wrote raw/orders/date=2026-10-04/part-0.jsonl: 200 orders
   13.1ms  MainThread  wrote raw/orders/date=2026-10-05/part-0.jsonl: 200 orders
   14.5ms  MainThread  wrote raw/orders/date=2026-10-06/part-0.jsonl: 200 orders
   15.9ms  MainThread  wrote raw/orders/date=2026-10-07/part-0.jsonl: 200 orders

day          orders       amount
2026-10-01      200    20,167.27
2026-10-02      200    19,896.64
2026-10-03      200    19,642.69
2026-10-04      200    20,796.47
2026-10-05      200    21,163.00
2026-10-06      200    19,791.23
2026-10-07      200    20,748.32
```

- Raw data is the input every job reads, and it is never edited. Each file is created in mode `"x"`, which fails
  if the file exists. Run `dsas raw` again, and every day says it stays as it is.
- Data that arrives later, such as a late order or a correction, lands as a new file in its day's folder,
  `part-1.jsonl` and up. So a job can always be rerun on exactly the input it read before.

## Layout

```
src/data_systems_at_scale/
  cli.py        the dsas command; each concept adds a subcommand
  pipelines/    jobs that turn raw data into outputs, and how to fix the outputs
    raw.py      seeded raw orders, one folder per day, written once
tests/          one behavior test per concept
```
