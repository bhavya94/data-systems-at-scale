import os
from datetime import date, timedelta

import pytest

from data_systems_at_scale.pipelines.backfill import plan, run, stale_days
from data_systems_at_scale.pipelines.jobs import customer_sales, daily_sales, week_of, weekly_sales
from data_systems_at_scale.pipelines.raw import late_arrivals, next_part, orders, write_part
from data_systems_at_scale.pipelines.versions import live, stage, swap, verify

DAYS = [date(2026, 10, 1) + timedelta(days=i) for i in range(4)]
DAY = DAYS[2]
OUTPUT = f"out/daily_sales/date={DAY.isoformat()}/data.jsonl"


@pytest.fixture
def lake(tmp_path):
    for day in DAYS:
        write_part(tmp_path, day, orders(day, 30, seed=0))
        daily_sales(tmp_path, day)
    weekly_sales(tmp_path, week_of(DAY))
    customer_sales(tmp_path, week_of(DAY))
    late = late_arrivals(tmp_path, DAY, late=2, corrections=1, seed=0)
    write_part(tmp_path, DAY, late, part=next_part(tmp_path, DAY))
    return tmp_path


def _rebuild_staged(lake):
    staging, version = stage(lake)
    steps = plan(staging, stale_days(lake))
    run(staging, steps, parallel=2)
    return staging, version, steps


def test_readers_see_the_old_version_until_the_swap_then_the_new_one(lake):
    before = (lake / OUTPUT).read_bytes()
    staging, version, steps = _rebuild_staged(lake)
    assert verify(lake, staging, steps) == []
    assert (lake / OUTPUT).read_bytes() == before  # live: still the old version
    swap(lake, version)
    assert os.readlink(lake / "out") == version
    assert (lake / OUTPUT).read_bytes() != before  # live: the rebuilt day
    assert (lake / "out-v1" / OUTPUT.removeprefix("out/")).read_bytes() == before  # kept, to go back


def test_a_staged_output_that_does_not_add_up_fails_its_check(lake):
    staging, version, steps = _rebuild_staged(lake)
    path = staging / OUTPUT
    path.write_text(path.read_text().replace('"amount_cents": ', '"amount_cents": 1', 1))
    problems = verify(lake, staging, steps)
    assert [p.split(":")[0] for p in problems] == [f"daily_sales date={DAY.isoformat()}",
                                                   f"weekly_sales week={week_of(DAY)}"]  # its week no longer adds up
    assert live(lake).name == "out-v1"  # nothing swapped


def test_an_output_outside_the_plan_that_changed_fails_its_check(lake):
    staging, version, steps = _rebuild_staged(lake)
    other = staging / "out/daily_sales/date=2026-10-01/data.jsonl"
    other.write_text("".join(reversed(other.read_text().splitlines(keepends=True))))  # new bytes, same totals
    assert verify(lake, staging, steps) == ["daily_sales/date=2026-10-01: changed, but it wasn't in the plan"]
