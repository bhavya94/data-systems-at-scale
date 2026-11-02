import json
from datetime import date, timedelta

import pytest

from data_systems_at_scale.pipelines.backfill import plan, run, stale_days
from data_systems_at_scale.pipelines.deletes import copies, erase
from data_systems_at_scale.pipelines.jobs import customer_sales, daily_sales, week_of, weekly_sales
from data_systems_at_scale.pipelines.raw import day_folder, orders, write_part

DAYS = [date(2026, 10, 1) + timedelta(days=i) for i in range(5)]  # weeks 40 and 41


@pytest.fixture
def lake(tmp_path):
    for day in DAYS:
        write_part(tmp_path, day, orders(day, 40, seed=0))
        daily_sales(tmp_path, day)
    for week in sorted({week_of(day) for day in DAYS}):
        weekly_sales(tmp_path, week)
        customer_sales(tmp_path, week)
    return tmp_path


def _delete(lake, customer):
    erase(lake, customer)
    run(lake, plan(lake, stale_days(lake)), parallel=2)


def test_after_a_delete_no_file_mentions_the_customer(lake):
    assert copies(lake, "c-17")  # in raw files and in customer_sales
    _delete(lake, "c-17")
    assert copies(lake, "c-17") == []


def test_a_search_for_one_id_does_not_match_a_longer_one(tmp_path):
    (tmp_path / "other.jsonl").write_text(json.dumps({"customer": "c-170"}) + "\n")
    assert copies(tmp_path, "c-17") == []


def test_the_totals_drop_by_exactly_the_customers_orders(lake):
    day = DAYS[0]
    theirs = [o for o in orders(day, 40, seed=0) if o["customer"] == "c-17"]
    before = sum(json.loads(r)["amount_cents"] for r in (lake / "out/daily_sales/date=2026-10-01/data.jsonl")
                 .read_text().splitlines())
    _delete(lake, "c-17")
    after = sum(json.loads(r)["amount_cents"] for r in (lake / "out/daily_sales/date=2026-10-01/data.jsonl")
                .read_text().splitlines())
    assert before - after == sum(o["amount_cents"] for o in theirs)


def test_other_customers_and_untouched_files_stay_as_they_were(lake):
    other = [r for r in (lake / "out/customer_sales/week=2026-W40/data.jsonl").read_text().splitlines()
             if '"c-03"' in r]
    untouched = {p: p.read_bytes() for p in sorted((lake / "raw").rglob("*.jsonl")) if b'"c-17"' not in p.read_bytes()}
    assert untouched  # c-17 has no orders on Oct 3 and 4
    _delete(lake, "c-17")
    assert [r for r in (lake / "out/customer_sales/week=2026-W40/data.jsonl").read_text().splitlines()
            if '"c-03"' in r] == other
    assert all(p.read_bytes() == data for p, data in untouched.items())
