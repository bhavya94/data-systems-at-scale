from datetime import date, timedelta

import pytest

from data_systems_at_scale.pipelines.backfill import plan, run
from data_systems_at_scale.pipelines.jobs import daily_sales, week_of, weekly_sales
from data_systems_at_scale.pipelines.raw import orders, write_part

DAYS = [date(2026, 10, 1) + timedelta(days=i) for i in range(7)]  # Oct 1-4 in ISO week 40, Oct 5-7 in week 41


@pytest.fixture
def lake(tmp_path):
    for day in DAYS:
        write_part(tmp_path, day, orders(day, 20, seed=0))
        daily_sales(tmp_path, day)
    for week in sorted({week_of(day) for day in DAYS}):
        weekly_sales(tmp_path, week)
    return tmp_path


def test_the_plan_covers_what_the_bad_days_reached_dailies_first(lake):
    steps = plan(lake, [date(2026, 10, 3), date(2026, 10, 5)])
    assert [(s.level, s.job, s.partition) for s in steps] == [
        (1, "daily_sales", "date=2026-10-03"), (1, "daily_sales", "date=2026-10-05"),
        (2, "weekly_sales", "week=2026-W40"), (2, "weekly_sales", "week=2026-W41")]
    assert steps[0].reads == 20  # the day's raw orders


def test_two_bad_days_in_one_week_rebuild_that_week_once(lake):
    steps = plan(lake, [date(2026, 10, 2), date(2026, 10, 3)])
    assert [s.partition for s in steps if s.job == "weekly_sales"] == ["week=2026-W40"]


def test_the_run_keeps_the_throttle_and_the_order(lake):
    done, peak = run(lake, plan(lake, [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)]), parallel=2,
                     step_ms=50)
    assert peak == 2  # 3 daily steps, at most 2 at once
    dailies = [d for d in done if d.step.level == 1]
    weeklies = [d for d in done if d.step.level == 2]
    assert max(d.end for d in dailies) <= min(d.start for d in weeklies)  # the week waits for its days
