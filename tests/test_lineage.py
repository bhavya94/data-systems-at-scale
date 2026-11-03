import json
from datetime import date, timedelta

import pytest

from data_systems_at_scale.pipelines.jobs import daily_sales, week_of, weekly_sales
from data_systems_at_scale.pipelines.lineage import downstream
from data_systems_at_scale.pipelines.raw import orders, write_part

START = date(2026, 10, 1)  # a Thursday: Oct 1-4 are in ISO week 40, Oct 5-7 in week 41
DAYS = [START + timedelta(days=i) for i in range(7)]


@pytest.fixture
def lake(tmp_path):
    for day in DAYS:
        write_part(tmp_path, day, orders(day, 20, seed=0))
        daily_sales(tmp_path, day)
    for week in sorted({week_of(day) for day in DAYS}):
        weekly_sales(tmp_path, week)
    return tmp_path


def test_each_output_records_its_inputs_and_their_fingerprints(lake):
    entry = json.loads((lake / "out/weekly_sales/week=2026-W41/_lineage.json").read_text())
    assert [i["path"] for i in entry["inputs"]] == [f"out/daily_sales/date=2026-10-0{d}/data.jsonl" for d in (5, 6, 7)]
    assert all(len(i["fingerprint"]) == 12 for i in entry["inputs"])


def test_a_bad_day_reaches_its_daily_output_and_its_week_only(lake):
    tree = downstream(lake, ["raw/orders/date=2026-10-03/part-0.jsonl"])
    assert tree == [(0, "raw/orders/date=2026-10-03/part-0.jsonl"),
                    (1, "out/daily_sales/date=2026-10-03/data.jsonl"),
                    (2, "out/weekly_sales/week=2026-W40/data.jsonl")]


def test_weekly_sales_adds_up_its_days(lake):
    week = [json.loads(line) for line in (lake / "out/weekly_sales/week=2026-W40/data.jsonl").read_text().splitlines()]
    assert sum(row["orders"] for row in week) == 4 * 20
