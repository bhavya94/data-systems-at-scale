from datetime import date

from data_systems_at_scale.pipelines.raw import day_folder, orders, read_day, write_part

DAY = date(2026, 10, 1)


def test_the_same_day_and_seed_give_the_same_orders():
    assert orders(DAY, 50, seed=0) == orders(DAY, 50, seed=0)
    assert orders(DAY, 50, seed=0) != orders(DAY, 50, seed=1)


def test_a_raw_file_is_written_once(tmp_path):
    first = write_part(tmp_path, DAY, orders(DAY, 5, seed=0))
    before = first.read_bytes()
    assert write_part(tmp_path, DAY, orders(DAY, 9, seed=1)) is None  # refused: the file exists
    assert first.read_bytes() == before


def test_a_day_reads_every_part_file(tmp_path):
    write_part(tmp_path, DAY, orders(DAY, 5, seed=0))
    write_part(tmp_path, DAY, orders(DAY, 2, seed=1), part=1)  # data that arrived later
    assert len(read_day(tmp_path, DAY)) == 7
    assert sorted(p.name for p in day_folder(tmp_path, DAY).iterdir()) == ["part-0.jsonl", "part-1.jsonl"]
