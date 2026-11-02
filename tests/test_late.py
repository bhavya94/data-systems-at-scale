import json
from datetime import date, timedelta

import pytest

from data_systems_at_scale.pipelines.backfill import plan, run, stale_days
from data_systems_at_scale.pipelines.jobs import daily_sales, week_of, weekly_sales
from data_systems_at_scale.pipelines.raw import late_arrivals, next_part, orders, read_day, write_part

DAYS = [date(2026, 10, 1) + timedelta(days=i) for i in range(4)]
DAY = DAYS[2]


@pytest.fixture
def lake(tmp_path):
    for day in DAYS:
        write_part(tmp_path, day, orders(day, 30, seed=0))
        daily_sales(tmp_path, day)
    weekly_sales(tmp_path, week_of(DAY))
    return tmp_path


def _totals(path):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    return sum(r["orders"] for r in rows), sum(r["amount_cents"] for r in rows)


def test_a_correction_replaces_the_old_amount_and_a_late_order_adds_one(lake):
    before = read_day(lake, DAY)
    arrivals = late_arrivals(lake, DAY, late=2, corrections=1, seed=0)
    write_part(lake, DAY, arrivals, part=next_part(lake, DAY))
    old = next(o for o in before if o["order_id"] == arrivals[-1]["order_id"])
    expected = sum(o["amount_cents"] for o in before) - old["amount_cents"] + sum(a["amount_cents"] for a in arrivals)
    assert _totals(daily_sales(lake, DAY)) == (30 + 2, expected)


def test_only_the_changed_day_is_stale_until_its_backfill(lake):
    assert stale_days(lake) == []
    write_part(lake, DAY, late_arrivals(lake, DAY, late=2, corrections=1, seed=0), part=next_part(lake, DAY))
    assert stale_days(lake) == [DAY]
    steps = plan(lake, stale_days(lake))
    assert steps[0].reads == 30 + 3  # the cost counts the late file: 2 late orders and 1 correction
    run(lake, steps, parallel=2)
    assert stale_days(lake) == []
    assert _totals(lake / f"out/weekly_sales/week={week_of(DAY)}/data.jsonl")[0] == 4 * 30 + 2  # the week caught up


def test_an_old_version_replayed_after_its_correction_does_not_undo_it(lake):
    original = read_day(lake, DAY)[0]
    corrected = dict(original, amount_cents=original["amount_cents"] + 1000, version=2)
    write_part(lake, DAY, [corrected], part=next_part(lake, DAY))
    after_correction = _totals(daily_sales(lake, DAY))
    write_part(lake, DAY, [original], part=next_part(lake, DAY))  # the old version arrives again, later
    assert _totals(daily_sales(lake, DAY)) == after_correction
