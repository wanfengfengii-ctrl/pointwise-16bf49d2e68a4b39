"""Three independently committed SQLite materializations; no backup implementation."""
import hashlib
import json
import sqlite3

NODES = ("raw", "clean", "daily")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


class Store:
    def __init__(self, connection, node, network):
        if node not in NODES or not isinstance(network, str) or not network:
            raise ValueError("BAD_IDENTITY")
        self.db = connection
        self.db.isolation_level = None
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=3000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS identity(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),
          network TEXT NOT NULL, node TEXT NOT NULL, head INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS frames(
          seq INTEGER PRIMARY KEY, clock TEXT NOT NULL, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS receipts(
          request TEXT PRIMARY KEY, digest TEXT NOT NULL, seq INTEGER NOT NULL);
        """)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            ident = self.db.execute("SELECT network,node FROM identity").fetchone()
            if ident is None:
                self.db.execute("INSERT INTO identity VALUES(1,?,?,0)", (network, node))
                self.db.execute("INSERT INTO frames VALUES(0,?,?)",
                                (canonical(dict.fromkeys(NODES, 0)), "{}"))
            elif ident != (network, node):
                raise ValueError("IDENTITY_MISMATCH")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.node = node
        self.network = network

    def snapshot(self, seq=None):
        # One SELECT: head, data and cumulative dependency vector are atomic.
        if seq is None:
            row = self.db.execute(
                "SELECT f.seq,f.clock,f.data FROM frames f JOIN identity i ON f.seq=i.head"
            ).fetchone()
        else:
            row = self.db.execute(
                "SELECT seq,clock,data FROM frames WHERE seq=?", (seq,)).fetchone()
        if row is None:
            raise ValueError("HISTORY_GONE")
        return {"seq": row[0], "clock": json.loads(row[1]), "rows": json.loads(row[2])}

    def commit(self, request, payload, transform, observed=()):
        if not isinstance(request, str) or not request:
            raise ValueError("BAD_REQUEST")
        fingerprint = digest(payload)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            receipt = self.db.execute(
                "SELECT digest,seq FROM receipts WHERE request=?", (request,)).fetchone()
            if receipt:
                if receipt[0] != fingerprint:
                    raise ValueError("REQUEST_CONFLICT")
                self.db.commit()
                return {"node": self.node, "seq": receipt[1], "request": request}
            before = self.snapshot()
            rows = transform(before["rows"])
            clock = before["clock"]
            for snapshot in observed:
                for node in NODES:
                    clock[node] = max(clock[node], snapshot["clock"][node])
            seq = before["seq"] + 1
            clock[self.node] = seq
            self.db.execute("INSERT INTO frames VALUES(?,?,?)",
                            (seq, canonical(clock), canonical(rows)))
            self.db.execute("INSERT INTO receipts VALUES(?,?,?)", (request, fingerprint, seq))
            self.db.execute("UPDATE identity SET head=?", (seq,))
            self.db.commit()
            return {"node": self.node, "seq": seq, "request": request}
        except BaseException:
            self.db.rollback()
            raise

    def trim(self, keep):
        if type(keep) is not int or keep < 1:
            raise ValueError("BAD_KEEP")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            head = self.db.execute("SELECT head FROM identity").fetchone()[0]
            self.db.execute("DELETE FROM frames WHERE seq < ?", (max(0, head - keep + 1),))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise


class Pipeline:
    def __init__(self, stores):
        if set(stores) != set(NODES):
            raise ValueError("MISSING_STORE")
        if len({s.network for s in stores.values()}) != 1:
            raise ValueError("NETWORK_MISMATCH")
        self.stores = stores

    def load(self, request, batch):
        if not isinstance(batch, list):
            raise ValueError("BAD_BATCH")
        def apply(rows):
            seen = set()
            for record in batch:
                if not isinstance(record, dict) or set(record) != {
                    "id", "rev", "station", "day", "value", "deleted"
                }:
                    raise ValueError("BAD_RECORD")
                if any(not isinstance(record[k], str) or not record[k]
                       for k in ("id", "station")):
                    raise ValueError("BAD_RECORD")
                if any(type(record[k]) is not int for k in ("rev", "day", "value")):
                    raise ValueError("BAD_RECORD")
                if record["rev"] < 1 or type(record["deleted"]) is not bool:
                    raise ValueError("BAD_RECORD")
                key = record["id"]
                if key in seen:
                    raise ValueError("DUPLICATE_ID")
                seen.add(key)
                old = rows.get(key)
                if old and old["rev"] == record["rev"] and old != record:
                    raise ValueError("REVISION_CONFLICT")
                if old is None or record["rev"] > old["rev"]:
                    rows[key] = dict(record)
            return rows
        return self.stores["raw"].commit(request, {"load": batch}, apply)

    def clean(self, request, factor):
        if type(factor) is not int:
            raise ValueError("BAD_FACTOR")
        raw = self.stores["raw"].snapshot()
        rows = {}
        for key, record in raw["rows"].items():
            if not record["deleted"]:
                rows[key] = {"station": record["station"], "day": record["day"],
                             "value": record["value"] * factor}
        return self.stores["clean"].commit(
            request, {"factor": factor}, lambda old: rows, (raw,))

    def daily(self, request):
        clean = self.stores["clean"].snapshot()
        rows = {}
        for record in clean["rows"].values():
            key = canonical([record["station"], record["day"]])
            group = rows.setdefault(key, {"station": record["station"], "day": record["day"],
                                          "total": 0, "count": 0})
            group["total"] += record["value"]
            group["count"] += 1
        return self.stores["daily"].commit(
            request, {"daily": True}, lambda old: rows, (clean,))

    def acknowledge(self, request):
        daily = self.stores["daily"].snapshot()
        # The intake operator records review of the downstream daily result.
        # This is a causal feedback commit, not a change to observations.
        return self.stores["raw"].commit(
            request, {"acknowledge": True}, lambda rows: rows, (daily,))

    def view(self):
        return {node: self.stores[node].snapshot() for node in NODES}


def open_pipeline(root, network=None):
    from pathlib import Path
    root = Path(root)
    if network is None:
        network = json.loads((root / "network.json").read_text(encoding="utf-8"))["network"]
    root.mkdir(parents=True, exist_ok=True)
    stores = {}
    try:
        for node in NODES:
            stores[node] = Store(sqlite3.connect(str(root / (node + ".sqlite"))),
                                 node, network)
        return Pipeline(stores)
    except BaseException:
        for store in stores.values():
            store.db.close()
        raise


def close_pipeline(pipeline):
    for store in pipeline.stores.values():
        store.db.close()
