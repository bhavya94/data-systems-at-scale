import json
from datetime import date

import pytest

from data_systems_at_scale.pipelines.jobs import daily_sales
from data_systems_at_scale.pipelines.raw import orders, read_day, write_part

DAY = date(2026, 10, 1)


@pytest.fixture
def lake(tmp_path):
    write_part(tmp_path, DAY, orders(DAY, 60, seed=0))
    return tmp_path


def test_daily_sales_adds_up_the_raw_orders(lake):
    totals = [json.loads(line) for line in daily_sales(lake, DAY).read_text().splitlines()]
    raw = read_day(lake, DAY)
    assert {t["store"] for t in totals} == {o["store"] for o in raw}
    assert sum(t["orders"] for t in totals) == len(raw)
    assert sum(t["amount_cents"] for t in totals) == sum(o["amount_cents"] for o in raw)


def test_a_rerun_gives_the_same_bytes(lake):
    first = daily_sales(lake, DAY).read_bytes()
    assert daily_sales(lake, DAY).read_bytes() == first


def test_a_crash_midway_leaves_the_old_output_whole(lake):
    before = daily_sales(lake, DAY).read_bytes()
    write_part(lake, DAY, orders(DAY, 10, seed=1), part=1)  # new input, so a finished run would change the output
    with pytest.raises(RuntimeError):
        daily_sales(lake, DAY, crash_midway=True)
    assert (lake / "out/daily_sales/date=2026-10-01/data.jsonl").read_bytes() == before


def test_the_output_depends_on_the_orders_not_their_order(tmp_path):
    rows = orders(DAY, 60, seed=0)
    forward, backward = tmp_path / "a", tmp_path / "b"
    write_part(forward, DAY, rows)
    write_part(backward, DAY, rows[::-1])
    assert daily_sales(forward, DAY).read_bytes() == daily_sales(backward, DAY).read_bytes()
