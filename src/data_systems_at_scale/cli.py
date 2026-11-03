"""`dsas`: the one command. Each concept the repo adds becomes a subcommand here."""

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

from data_systems_at_scale.pipelines.jobs import daily_sales, week_of, weekly_sales
from data_systems_at_scale.pipelines.lineage import downstream
from data_systems_at_scale.pipelines.raw import day_folder, days, orders, read_day, write_part
from data_systems_at_scale.pipelines.storage import fingerprint

# Each log line starts with the ms since the program started, then the thread that wrote it.
LOG_FORMAT = "%(relativeCreated)7.1fms  %(threadName)-10s  %(message)s"


def _at_least(minimum: int):
    def parse(text: str) -> int:
        if (value := int(text)) < minimum:
            raise argparse.ArgumentTypeError(f"must be {minimum} or more, not {value}")
        return value
    return parse


def _days(args: argparse.Namespace) -> list[date]:
    return [args.start + timedelta(days=i) for i in range(args.days)]


def _raw(args: argparse.Namespace) -> None:
    for day in _days(args):
        write_part(args.root, day, orders(day, args.rows, args.seed))
    print(f"\n{'day':<12} {'orders':>6} {'amount':>12}")
    for day in _days(args):
        rows = read_day(args.root, day)
        print(f"{day.isoformat():<12} {len(rows):>6} {sum(r['amount_cents'] for r in rows) / 100:>12,.2f}")


def _daily(args: argparse.Namespace) -> None:
    for day in args.day or days(args.root):
        try:
            path = daily_sales(args.root, day, args.crash_midway)
        except RuntimeError as crash:
            path = args.root / "out" / "daily_sales" / f"date={day.isoformat()}" / "data.jsonl"
            print(f"\n{crash}. The output is still {fingerprint(path) or 'missing'}, whole; "
                  f"{path.name}.tmp is left behind, and nothing reads it.")
            return
    print(f"\n{path.relative_to(args.root)}:")
    print(path.read_text(), end="")


def _weekly(args: argparse.Namespace) -> None:
    for week in sorted({week_of(day) for day in days(args.root)}):
        weekly_sales(args.root, week)


def _lineage(args: argparse.Namespace) -> None:
    starts = sorted(str(p.relative_to(args.root)) for p in day_folder(args.root, args.bad_day).glob("part-*.jsonl"))
    tree = downstream(args.root, starts)
    print(f"\neverything {args.bad_day} reached:")
    for depth, path in tree:
        print("   " * (depth - 1) + ("└─ " if depth else "") + path)
    print(f"\n{len(tree) - len(starts)} outputs to fix")


def main() -> None:
    lake = argparse.ArgumentParser(add_help=False)  # every concept reads and writes the same lake
    lake.add_argument("--root", type=Path, default=Path("lake"), help="where the data lives (default lake)")
    parser = argparse.ArgumentParser(prog="dsas", description="Run one concept and watch what it does.")
    concepts = parser.add_subparsers(dest="concept", title="concepts")
    raw = concepts.add_parser("raw", parents=[lake], help="write seeded raw orders, one folder per day")
    raw.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 1),
                     help="first day (default 2026-10-01)")
    raw.add_argument("--days", type=_at_least(1), default=7, help="how many days (default 7)")
    raw.add_argument("--rows", type=_at_least(1), default=200, help="orders per day (default 200)")
    raw.add_argument("--seed", type=int, default=0, help="the same seed gives the same orders (default 0)")
    raw.set_defaults(run=_raw)
    daily = concepts.add_parser("daily", parents=[lake], help="daily sales per store; safe to rerun")
    daily.add_argument("--day", type=date.fromisoformat, action="append",
                       help="a day to run, repeatable (default: every raw day)")
    daily.add_argument("--crash-midway", action="store_true", help="crash halfway through writing the output")
    daily.set_defaults(run=_daily)
    weekly = concepts.add_parser("weekly", parents=[lake], help="weekly sales per store, from the daily outputs")
    weekly.set_defaults(run=_weekly)
    lineage = concepts.add_parser("lineage", parents=[lake], help="every output a bad raw day reached")
    lineage.add_argument("--bad-day", type=date.fromisoformat, required=True, help="the raw day that was wrong")
    lineage.set_defaults(run=_lineage)
    args = parser.parse_args()
    if args.concept is None:
        parser.print_help()
        return
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    args.run(args)
