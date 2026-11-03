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

## Lineage: trace a bad day to everything it reached

Every job now records its inputs beside its output, in `_lineage.json`. A second job, `dsas weekly`, adds up
the daily outputs of each ISO week. So a raw day feeds a daily output, which feeds a weekly one.

```console
$ uv run dsas raw >/dev/null 2>&1; uv run dsas daily >/dev/null 2>&1
$ uv run dsas weekly 2>&1 | grep "lineage:"
   14.4ms  MainThread  lineage: out/weekly_sales/week=2026-W40/data.jsonl read 4 inputs: out/daily_sales/date=2026-10-01/data.jsonl, out/daily_sales/date=2026-10-02/data.jsonl, out/daily_sales/date=2026-10-03/data.jsonl, out/daily_sales/date=2026-10-04/data.jsonl
   22.5ms  MainThread  lineage: out/weekly_sales/week=2026-W41/data.jsonl read 3 inputs: out/daily_sales/date=2026-10-05/data.jsonl, out/daily_sales/date=2026-10-06/data.jsonl, out/daily_sales/date=2026-10-07/data.jsonl

$ head -9 lake/out/weekly_sales/week=2026-W40/_lineage.json
{
 "inputs": [
  {
   "fingerprint": "a3db0c49931f",
   "path": "out/daily_sales/date=2026-10-01/data.jsonl"
  },
  {
   "fingerprint": "71292aa02f9e",
   "path": "out/daily_sales/date=2026-10-02/data.jsonl"
```

Say the raw orders for 2026-10-03 turn out to be wrong. Lineage answers: which outputs did they reach?

```console
$ uv run dsas lineage --bad-day 2026-10-03

everything 2026-10-03 reached:
raw/orders/date=2026-10-03/part-0.jsonl
└─ out/daily_sales/date=2026-10-03/data.jsonl
   └─ out/weekly_sales/week=2026-W40/data.jsonl

2 outputs to fix
```

- `downstream` reads every `_lineage.json`, finds the outputs that read the bad file, then the outputs that read
  those, and so on.
- 2026-10-03 reached its own daily output and week 40, and nothing else. Only those two need fixing.
- Each input's fingerprint shows exactly which version of it the output read. A later step uses that to find
  outputs whose inputs changed.

## Backfill: scope, order, cost and throttle

Say three raw days were wrong. `dsas backfill` plans the rebuild of everything they reached, shows the cost,
and runs it a few steps at a time.

```console
$ uv run dsas backfill --bad-day 2026-10-02 --bad-day 2026-10-03 --bad-day 2026-10-05 --dry-run

level  job           partition        rows read
    1  daily_sales   date=2026-10-02        200
    1  daily_sales   date=2026-10-03        200
    1  daily_sales   date=2026-10-05        200
    2  weekly_sales  week=2026-W40           12
    2  weekly_sales  week=2026-W41            9
5 steps, 621 rows to read

dry run: nothing rebuilt
```

- Scope: lineage finds the daily output of each bad day, and the weeks those days feed. Weeks 40 and 41 each
  appear once, though week 40 holds two bad days.
- Order: a weekly output reads daily outputs, so every daily step is level 1 and runs before any level 2 step.
- Cost: each step shows the rows it reads. `--dry-run` stops here, so you see the bill before you pay it.

Then the run, with at most 2 steps at once. `--step-ms 200` makes each step take 200 ms longer, as a day of real
data would, so you can watch the throttle:

```console
$ uv run dsas backfill --bad-day 2026-10-02 --bad-day 2026-10-03 --bad-day 2026-10-05 --step-ms 200 2>&1 \
    | grep -E "rebuilding|done:|rebuilt|at most"
   11.2ms  backfill_0  rebuilding daily_sales date=2026-10-02 (1 running, limit 2)
   11.3ms  backfill_1  rebuilding daily_sales date=2026-10-03 (2 running, limit 2)
  221.3ms  backfill_1  rebuilding daily_sales date=2026-10-05 (2 running, limit 2)
  430.4ms  MainThread  level 1 done: rebuilt 3, so the next level can read them
  430.7ms  backfill_0  rebuilding weekly_sales week=2026-W40 (1 running, limit 2)
  430.8ms  backfill_1  rebuilding weekly_sales week=2026-W41 (2 running, limit 2)
  640.3ms  MainThread  level 2 done: rebuilt 2, so the next level can read them
rebuilt 5 outputs in 629 ms; at most 2 ran at once (limit 2)
```

- `backfill_0` and `backfill_1` take the first two days. The third day starts only when one of them finishes.
- The weekly steps start only once level 1 is done, so they read the rebuilt days.
- A lock guards the count of running steps, because every backfill thread updates it.

## Layout

```
src/data_systems_at_scale/
  cli.py        the dsas command: raw, daily, weekly, lineage and backfill
  pipelines/    jobs that turn raw data into outputs, and how to fix the outputs
    raw.py      seeded raw orders, one folder per day, written once
    jobs.py     daily and weekly sales per store
    lineage.py  each output's inputs; everything a bad input reached
    backfill.py plan and run a rebuild: scope, order, cost and throttle
    storage.py  write a temp file, then rename it over the output
tests/          one behavior test per concept
```
