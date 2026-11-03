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

## Late and corrected orders

Orders for an old day can arrive after its file was written: late orders, and corrections of earlier ones. `dsas
late` drops them into the day's folder as a new file. A correction repeats an order's id, with a new amount and
the next `version`.

```console
$ uv run dsas late --day 2026-10-03
    9.5ms  MainThread  wrote raw/orders/date=2026-10-03/part-1.jsonl: 8 orders

raw/orders/date=2026-10-03/part-1.jsonl: 5 late orders, 3 corrections
{"amount_cents": 1340, "customer": "c-26", "event_time": "2026-10-03T02:35:54Z", "order_id": "1003-L0004", "store": "s-3"}
{"amount_cents": 1205, "customer": "c-30", "event_time": "2026-10-03T03:21:26Z", "order_id": "1003-L0003", "store": "s-1"}
{"amount_cents": 1604, "customer": "c-04", "event_time": "2026-10-03T03:28:11Z", "order_id": "1003-L0002", "store": "s-3"}
{"amount_cents": 11028, "customer": "c-17", "event_time": "2026-10-03T13:19:11Z", "order_id": "1003-L0000", "store": "s-3"}
{"amount_cents": 15785, "customer": "c-38", "event_time": "2026-10-03T15:28:35Z", "order_id": "1003-L0001", "store": "s-3"}
{"amount_cents": 583, "customer": "c-37", "event_time": "2026-10-03T05:00:59Z", "order_id": "1003-0020", "store": "s-2", "version": 2}
{"amount_cents": 5087, "customer": "c-24", "event_time": "2026-10-03T03:54:52Z", "order_id": "1003-0128", "store": "s-3", "version": 2}
{"amount_cents": 5244, "customer": "c-35", "event_time": "2026-10-03T21:44:04Z", "order_id": "1003-0124", "store": "s-1", "version": 2}
```

`dsas backfill --stale` finds the days whose raw files changed since their daily output read them. It compares
the fingerprints in each `_lineage.json` with the files as they are now. Then it rebuilds those days and the
weeks they feed.

```console
$ uv run dsas backfill --stale 2>&1 | grep -vE "wrote [0-9]+ bytes|lineage:|rebuilding|adding up"
    9.2ms  MainThread  stale: out/daily_sales/date=2026-10-03/data.jsonl: the folder now holds 2 raw files, and it read 1

days to backfill: 2026-10-03

level  job           partition        rows read
    1  daily_sales   date=2026-10-03        208
    2  weekly_sales  week=2026-W40           12
2 steps, 220 rows to read

   11.3ms  backfill_0  daily_sales 2026-10-03: read 208 raw orders; output was c3b772c8e6c6
   11.3ms  backfill_0  daily_sales 2026-10-03: 3 of them correct earlier orders; the latest version of each counts
   12.1ms  backfill_0  daily_sales 2026-10-03: output is a86eb8f79d1a, changed
   12.2ms  MainThread  level 1 done: rebuilt 1, so the next level can read them
   13.2ms  backfill_0  weekly_sales 2026-W40: output is b4992bb91d7f, changed
   13.3ms  MainThread  level 2 done: rebuilt 1, so the next level can read them

rebuilt 2 outputs in 3 ms; at most 1 ran at once (limit 2)

$ uv run dsas backfill --stale

nothing to backfill: every output read the raw files as they are now
```

- Only 2026-10-03 is stale. The other days' files match what their outputs read, so they aren't touched.
- The day now reads 208 raw orders, and 3 of them correct earlier ones. Only the latest version of each order
  counts, so a correction replaces the old amount instead of adding to it. An old version that arrives again
  later doesn't undo a correction.
- The plan's cost counts the late file too: 208 rows, not the 200 the old output read.
- Run it again, and nothing is stale.

## Deletes that reach every copy

A customer asks to be forgotten. Their orders sit in raw files, and their id sits in `customer_sales`, a weekly
table of orders per customer that `dsas weekly` now builds too. Their amounts also sit inside every sales total.

```console
$ uv run dsas delete --customer c-17 2>&1 | grep -vE "wrote [0-9]+ bytes|lineage:|rebuilding [a-z]+_sales|adding up|read [0-9]+ raw orders|customer_sales [0-9W-]+: [0-9]+ orders|output is|stale: .*date=2026-10-0[2-7]"

c-17 appears in 9 files:
  out/customer_sales/week=2026-W40/data.jsonl
  out/customer_sales/week=2026-W41/data.jsonl
  raw/orders/date=2026-10-01/part-0.jsonl
  raw/orders/date=2026-10-02/part-0.jsonl
  raw/orders/date=2026-10-03/part-0.jsonl
  raw/orders/date=2026-10-04/part-0.jsonl
  raw/orders/date=2026-10-05/part-0.jsonl
  raw/orders/date=2026-10-06/part-0.jsonl
  raw/orders/date=2026-10-07/part-0.jsonl

   53.9ms  MainThread  erased 5 orders of c-17 from raw/orders/date=2026-10-01/part-0.jsonl; 195 remain
   55.9ms  MainThread  erased 7 orders of c-17 from raw/orders/date=2026-10-02/part-0.jsonl; 193 remain
   59.1ms  MainThread  erased 6 orders of c-17 from raw/orders/date=2026-10-03/part-0.jsonl; 194 remain
   60.0ms  MainThread  erased 4 orders of c-17 from raw/orders/date=2026-10-04/part-0.jsonl; 196 remain
   61.4ms  MainThread  erased 7 orders of c-17 from raw/orders/date=2026-10-05/part-0.jsonl; 193 remain
   62.6ms  MainThread  erased 7 orders of c-17 from raw/orders/date=2026-10-06/part-0.jsonl; 193 remain
   65.0ms  MainThread  erased 5 orders of c-17 from raw/orders/date=2026-10-07/part-0.jsonl; 195 remain

erased 41 orders from 7 raw files; rebuilding what they reached

   67.4ms  MainThread  stale: out/daily_sales/date=2026-10-01/data.jsonl: a raw file it read has different bytes now
  112.0ms  MainThread  level 1 done: rebuilt 9, so the next level can read them
  113.8ms  MainThread  level 2 done: rebuilt 2, so the next level can read them

c-17 now appears in 0 files
```

1. Find: c-17 appears in 7 raw files and both weeks of `customer_sales`.
2. Rewrite raw: this is the one exception to "raw is written once". Each raw file that holds the customer is
   rewritten without them, in one step: a temp file, then a rename.
3. Rebuild: the rewritten files have new fingerprints, so every day is stale. The stale backfill rebuilds the
   days, the weeks and `customer_sales`. The grep above keeps one stale line of the seven.
4. Verify: a scan of every file in the lake finds the id nowhere. It searches for `"c-17"` with its quotes, so
   it would not match `c-170`.

Each total drops by exactly the deleted orders, and files without the customer keep their bytes.

## Layout

```
src/data_systems_at_scale/
  cli.py        the dsas command: raw, daily, weekly, lineage, backfill, late and delete
  pipelines/    jobs that turn raw data into outputs, and how to fix the outputs
    raw.py      seeded raw orders, one folder per day, written once; late arrivals
    jobs.py     daily and weekly sales per store, and weekly sales per customer
    lineage.py  each output's inputs; everything a bad input reached
    backfill.py plan and run a rebuild: scope, order, cost and throttle; stale days
    deletes.py  remove one customer from raw data and everything built from it
    storage.py  write a temp file, then rename it over the output
tests/          one behavior test per concept
```
