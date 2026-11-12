import json
import sqlite3
from pipeline import NODES, Pipeline, Store
from samples import INITIAL, CORRECTION


def memory_pipeline():
    return Pipeline({n: Store(sqlite3.connect(":memory:"), n, "sample-water") for n in NODES})


def normal():
    p = memory_pipeline()
    try:
        assert p.load("raw-1", INITIAL)["seq"] == 1
        assert p.clean("clean-1", 2)["seq"] == 1
        assert p.daily("daily-1")["seq"] == 1
        v = p.view()
        assert v["daily"]["clock"] == {"raw": 1, "clean": 1, "daily": 1}
        groups = sorted(v["daily"]["rows"].values(), key=lambda r: (r["day"], r["station"]))
        assert groups == [
            {"station": "east", "day": 0, "total": 30, "count": 2},
            {"station": "west", "day": 0, "total": 14, "count": 1},
        ]
        assert p.load("raw-2", CORRECTION)["seq"] == 2
        p.clean("clean-2", 2)
        p.daily("daily-2")
        v = p.view()
        groups = sorted(v["daily"]["rows"].values(), key=lambda r: (r["day"], r["station"]))
        assert groups == [
            {"station": "east", "day": 0, "total": 10, "count": 1},
            {"station": "west", "day": 1, "total": -6, "count": 1},
        ]
        assert v["daily"]["clock"] == {"raw": 2, "clean": 2, "daily": 2}
        assert p.load("raw-1", INITIAL) == {"node": "raw", "seq": 1, "request": "raw-1"}
        assert p.clean("clean-1", 2)["seq"] == 1
        assert p.daily("daily-1")["seq"] == 1
        assert p.view() == v
        try:
            p.clean("clean-1", 3)
            raise AssertionError("request conflict accepted")
        except ValueError as error:
            assert str(error) == "REQUEST_CONFLICT"
        invalid = [dict(INITIAL[0], rev=3), dict(INITIAL[2], value=999)]
        try:
            p.load("invalid", invalid)
            raise AssertionError("revision conflict accepted")
        except ValueError as error:
            assert str(error) == "REVISION_CONFLICT"
        assert p.view() == v
        assert p.stores["raw"].db.execute(
            "SELECT count(*) FROM receipts WHERE request='invalid'").fetchone()[0] == 0
        assert p.acknowledge("review-2")["seq"] == 3
        assert p.stores["raw"].snapshot()["rows"] == v["raw"]["rows"]
        assert p.stores["raw"].snapshot()["clock"] == {"raw": 3, "clean": 2, "daily": 2}
        assert p.acknowledge("review-2")["seq"] == 3
        # A captured daily frame can require a raw frame newer than an earlier raw capture.
        early = p.stores["raw"].snapshot()
        p.load("raw-3", [dict(INITIAL[2], rev=3, value=8)])
        p.clean("clean-3", 2)
        p.daily("daily-3")
        late = p.stores["daily"].snapshot()
        assert late["clock"]["raw"] > early["seq"]
        # Ordinary retention removes history but never idempotency receipts.
        p.stores["raw"].trim(1)
        try:
            p.stores["raw"].snapshot(2)
            raise AssertionError("trim kept unexpected history")
        except ValueError as error:
            assert str(error) == "HISTORY_GONE"
        assert p.load("raw-1", INITIAL)["seq"] == 1
        assert p.stores["raw"].snapshot()["seq"] == 4
        print("NORMAL_REGRESSION_OK")
        print(json.dumps({"daily_after_correction": groups,
                          "nonclosed_capture": {"raw": early["seq"], "daily": late["seq"],
                                                "required_raw": late["clock"]["raw"]}},
                         ensure_ascii=False, sort_keys=True))
    finally:
        for store in p.stores.values():
            store.db.close()


if __name__ == "__main__":
    normal()
