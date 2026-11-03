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

## A job you can rerun safely

`dsas daily` reads one day's raw orders and writes that day's orders and amount per store. Run it, then run it
again:

```console
$ uv run dsas daily --day 2026-10-01
    7.3ms  MainThread  daily_sales 2026-10-01: read 200 raw orders; output was missing
    7.7ms  MainThread  wrote 231 bytes to data.jsonl.tmp; renaming it over data.jsonl in one step
    8.0ms  MainThread  daily_sales 2026-10-01: output is a3db0c49931f, new

out/daily_sales/date=2026-10-01/data.jsonl:
{"amount_cents": 625870, "date": "2026-10-01", "orders": 68, "store": "s-1"}
{"amount_cents": 767533, "date": "2026-10-01", "orders": 72, "store": "s-2"}
{"amount_cents": 623324, "date": "2026-10-01", "orders": 60, "store": "s-3"}

$ uv run dsas daily --day 2026-10-01 2>&1 | grep "output is"
    9.1ms  MainThread  daily_sales 2026-10-01: output is a3db0c49931f, the same bytes as before
```

- The job replaces its whole output for the day, and it reads only raw input, which never changes. So a rerun
  gives the same bytes: the fingerprint, the start of the file's SHA-256, stays `a3db0c49931f`.
- It never writes into the output itself. It writes a temp file beside it, then renames the temp file over the
  output with `os.replace`. A rename is one step: a reader sees the old file or the new one, never half of each.

`--crash-midway` stops halfway through writing the temp file, as a crash would:

```console
$ uv run dsas daily --day 2026-10-01 --crash-midway
    7.4ms  MainThread  daily_sales 2026-10-01: read 200 raw orders; output was a3db0c49931f

simulated crash after writing half of data.jsonl.tmp. The output is still a3db0c49931f, whole; data.jsonl.tmp is left behind, and nothing reads it.

$ wc -c lake/out/daily_sales/date=2026-10-01/data.jsonl*
     231 out/daily_sales/date=2026-10-01/data.jsonl
     115 out/daily_sales/date=2026-10-01/data.jsonl.tmp
     346 total
```

- The output keeps all its bytes. Only the temp file is half-written, and the next run replaces it.

## Layout

```
src/data_systems_at_scale/
  cli.py        the dsas command: raw and daily
  pipelines/    jobs that turn raw data into outputs, and how to fix the outputs
    raw.py      seeded raw orders, one folder per day, written once
    jobs.py     daily sales per store, from one day's raw orders
    storage.py  write a temp file, then rename it over the output
tests/          one behavior test per concept
```
