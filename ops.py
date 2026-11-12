"""Offline disaster-recovery operations for the three-store water pipeline.

This module is the ops component of the project entry points and of the
self-contained release package built by `ops.py build`:

  python3 -B ops.py build OUT
  python3 -B ops.py backup ROOT REPO ID
  python3 -B ops.py restore REPO ID DEST
  python3 -B ops.py prune REPO DROP.json

Repository layout (all acceptance/success/reference evidence ships in REPO):

  repo.json                     repository format marker
  objects/<sha256>              immutable content-addressed payload objects
  objects/tmp-*                 incomplete payload writes (reclaimable garbage)
  points/<ID>.json              committed restore-point manifests (never deleted)
  tasks/<ID>/accept.json        acceptance record, persisted before any
                                business payload is read or copied
  tasks/<ID>/capture-<n>.json   capture evidence, one file per capture attempt
  tasks/<ID>/state.json         held-object references (GC liveness evidence)
  tasks/<ID>/commit.json        committed result (success evidence)
  tasks/<ID>/task.lock          per-task serialization (same ID never races)
  restores/<token>.json         in-progress restore protections
  roots/<branch>.json           restored-branch anchors (incremental base refs)
  prune/retired.json            retired point IDs
  locks/gc.lock                 short metadata-section lock; never held across
                                payload I/O, so independent tasks run in parallel
"""
import argparse
import bisect
import fcntl
import hashlib
import importlib
import io
import json
import os
import signal
import sqlite3
import sys
import tempfile
import time
import urllib.parse
import uuid
import zipfile
from pathlib import Path

from pipeline import NODES, STORE_DDL, canonical, digest

REPO_FORMAT = 1
RELEASE_NAME = "water-ops-release"
RELEASE_MODULES = ("ops", "pipeline", "run", "samples")
MARKER = ".ops-restore.json"

RELEASE_MAIN = '''import importlib
import json
import sys


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("run", "ops"):
        raise ValueError("COMPONENT_REQUIRED")
    component = sys.argv.pop(1)
    importlib.import_module(component).main()


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        raise SystemExit(2)
'''


class OpsError(ValueError):
    """A handled operational failure; reported as {"error": ...}."""


# ---------------------------------------------------------------------------
# Test hooks (inert unless the environment variables are set): deterministic
# real-process kills at named persistence boundaries and small delays that
# widen race windows for the concurrency regression.

def _crash_point(name):
    if os.environ.get("OPS_CRASH_AT") == name:
        os.kill(os.getpid(), signal.SIGKILL)


def _slow():
    ms = os.environ.get("OPS_SLOW_MS")
    if ms:
        time.sleep(int(ms) / 1000.0)


# ---------------------------------------------------------------------------
# Small durable-file and locking helpers.

def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _fsync_dir(path):
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".tmp-")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _write_json(path, value):
    _atomic_write(path, canonical(value).encode("utf-8"))


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


class _FileLock:
    """Advisory exclusive lock; released by the OS even on SIGKILL."""

    def __init__(self, path):
        self.path = Path(path)

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = open(self.path, "a+b")
        fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        try:
            fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
        finally:
            self.fh.close()
        return False


_ID_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")


def _check_id(task_id):
    if (not task_id or len(task_id) > 200 or task_id.startswith(".")
            or any(ch not in _ID_CHARS for ch in task_id)):
        raise OpsError("BAD_ID")


# ---------------------------------------------------------------------------
# build: reproducible single-file release package.

def _module_bytes(name):
    module = importlib.import_module(name)
    origin = module.__file__
    loader = module.__loader__
    if hasattr(loader, "get_data"):
        return loader.get_data(origin)
    with open(origin, "rb") as fh:
        return fh.read()


def cmd_build(out):
    out = Path(out)
    sources = {name: _module_bytes(name) for name in RELEASE_MODULES}
    release = {
        "format": REPO_FORMAT,
        "name": RELEASE_NAME,
        "modules": {
            name: hashlib.sha256(data).hexdigest()
            for name, data in sorted(sources.items())
        },
    }
    entries = {
        "__main__.py": RELEASE_MAIN.encode("utf-8"),
        "release.json": canonical(release).encode("utf-8"),
    }
    for name, data in sources.items():
        entries[name + ".py"] = data
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in sorted(entries):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o644 << 16
            archive.writestr(info, entries[name], compresslevel=9)
    data = buffer.getvalue()
    if out.parent and not out.parent.exists():
        out.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(out, data)
    return {"artifact": str(out), "sha256": hashlib.sha256(data).hexdigest()}


# ---------------------------------------------------------------------------
# Repository and source-root metadata.

def _ensure_repo(repo):
    repo = Path(repo)
    for sub in ("objects", "points", "tasks", "restores", "roots", "locks", "prune"):
        (repo / sub).mkdir(parents=True, exist_ok=True)
    marker = repo / "repo.json"
    if not marker.exists():
        _write_json(marker, {"format": REPO_FORMAT, "kind": "water-ops-repo"})
    return repo


def _require_repo(repo):
    repo = Path(repo)
    if not (repo / "repo.json").exists():
        raise OpsError("REPO_MISSING")
    return repo


def _load_retired(repo):
    path = repo / "prune" / "retired.json"
    if path.exists():
        return set(_read_json(path)["retired"])
    return set()


def _store_retired(repo, retired):
    _write_json(repo / "prune" / "retired.json", {"retired": sorted(retired)})


def _ro_connection(db_path):
    uri = "file:%s?mode=ro" % urllib.parse.quote(str(db_path), safe="/")
    con = sqlite3.connect(uri, uri=True)
    con.isolation_level = None
    con.execute("PRAGMA busy_timeout=5000")
    return con


def _read_source_identity(root):
    root = Path(root)
    network_file = root / "network.json"
    if not network_file.exists():
        raise OpsError("ROOT_MISSING")
    network = json.loads(network_file.read_text(encoding="utf-8"))["network"]
    heads = {}
    for node in NODES:
        db_path = root / (node + ".sqlite")
        if not db_path.exists():
            raise OpsError("SOURCE_DB_MISSING:" + node)
        con = _ro_connection(db_path)
        try:
            row = con.execute("SELECT network,node,head FROM identity").fetchone()
        finally:
            con.close()
        if row is None or row[0] != network or row[1] != node:
            raise OpsError("IDENTITY_MISMATCH")
        heads[node] = row[2]
    return {"network": network, "heads": heads}


def _root_lock(root):
    return _FileLock(Path(root) / "ops.lock")


def _ensure_root_branch(root, network):
    """Bind the source root to a persistent ops branch identity."""
    with _root_lock(root):
        path = Path(root) / "ops.json"
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("network") != network:
                raise OpsError("IDENTITY_MISMATCH")
            return data
        data = {"branch": "br-" + uuid.uuid4().hex, "network": network, "base": None}
        _write_json(path, data)
        return data


def _protect_path(root):
    return Path(root) / "ops-protect.json"


def _read_protect(root):
    path = _protect_path(root)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("tasks"), dict):
                return data
        except (ValueError, OSError):
            pass
    return {"tasks": {}}


def _register_protection(root, task_id, floors):
    with _root_lock(root):
        data = _read_protect(root)
        data["tasks"][task_id] = floors
        _write_json(_protect_path(root), data)


def _release_protection(root, task_id):
    with _root_lock(root):
        data = _read_protect(root)
        if data["tasks"].pop(task_id, None) is not None:
            _write_json(_protect_path(root), data)


# ---------------------------------------------------------------------------
# Causal-cut selection.

def _greatest_feasible(candidates, lower):
    """Greatest component-wise causally closed cut, or None.

    candidates: node -> {seq: clock}. A cut c is feasible when every node's
    candidate frame at c[node] has clock <= c in all components. Feasible
    vectors are closed under component-wise max, so iterating downward from
    the greatest candidate vector reaches the unique greatest feasible one.
    It must also cover the acceptance lower bound `lower`.
    """
    seqs = {node: sorted(candidates[node]) for node in NODES}
    if any(not seqs[node] for node in NODES):
        return None
    cut = {node: seqs[node][-1] for node in NODES}
    while True:
        bad = [node for node in NODES
               if any(candidates[node][cut[node]][other] > cut[other]
                      for other in NODES)]
        if not bad:
            if all(cut[node] >= lower[node] for node in NODES):
                return cut
            return None
        for node in bad:
            index = bisect.bisect_left(seqs[node], cut[node])
            if index == 0:
                return None
            cut[node] = seqs[node][index - 1]


def _frame_digest(clock, rows):
    return digest({"clock": clock, "rows": rows})


def _chain_manifests(repo, point_id):
    chain = []
    seen = set()
    pid = point_id
    while pid is not None:
        if pid in seen:
            raise OpsError("CHAIN_CYCLE")
        seen.add(pid)
        path = repo / "points" / (pid + ".json")
        if not path.exists():
            raise OpsError("CHAIN_BROKEN:" + pid)
        manifest = _read_json(path)
        chain.append(manifest)
        pid = manifest["base"]["point"]
    return chain


def _candidate_clocks(repo, base):
    candidates = {node: {} for node in NODES}
    if base["point"] is not None:
        for manifest in _chain_manifests(repo, base["point"]):
            for node in NODES:
                for seq, clock, _fd in manifest["frame_index"][node]:
                    candidates[node][seq] = clock
    return candidates


def _coverage_key(vector):
    return tuple(vector[node] for node in NODES)


def _select_base(repo, branch, root_ops, retired):
    """Latest committed point on this branch, else the restore anchor base."""
    best = None
    for path in sorted((repo / "points").glob("*.json")):
        manifest = _read_json(path)
        if manifest["id"] in retired or manifest["branch"] != branch:
            continue
        if (best is None or _coverage_key(manifest["capture_upper"])
                >= _coverage_key(best["capture_upper"])):
            best = manifest
    if best is not None:
        return {"point": best["id"], "vector": best["capture_upper"]}
    anchor = (root_ops or {}).get("base")
    if anchor:
        path = repo / "points" / (anchor["point"] + ".json")
        if not path.exists():
            raise OpsError("BASE_UNKNOWN:" + anchor["point"])
        manifest = _read_json(path)
        if manifest["id"] in retired:
            raise OpsError("BASE_RETIRED")
        # A restored branch increments from the restore cut, not from the
        # parent branch's later (different-branch) frames.
        return {"point": manifest["id"], "vector": manifest["cut"]}
    return {"point": None, "vector": {node: -1 for node in NODES}}


# ---------------------------------------------------------------------------
# Capture: one consistent read view per database, range-limited to the
# not-yet-backed-up increment of the chosen committed base point.

def _capture(root, base_vector):
    root = Path(root)
    out = {}
    for node in NODES:
        con = _ro_connection(root / (node + ".sqlite"))
        try:
            con.execute("BEGIN")
            head = con.execute("SELECT head FROM identity").fetchone()[0]
            low = base_vector[node]
            frames = con.execute(
                "SELECT seq,clock,data FROM frames WHERE seq>? AND seq<=? ORDER BY seq",
                (low, head)).fetchall()
            receipts = con.execute(
                "SELECT request,digest,seq FROM receipts WHERE seq>? AND seq<=? ORDER BY seq",
                (low, head)).fetchall()
            con.execute("COMMIT")
        finally:
            con.close()
        out[node] = {
            "upper": head,
            "frames": [{"seq": seq, "clock": json.loads(clock), "rows": json.loads(data)}
                       for seq, clock, data in frames],
            "receipts": [{"request": request, "digest": dgst, "seq": seq}
                         for request, dgst, seq in receipts],
        }
        _slow()
        _crash_point("capture")
    return out


def _capture_loop(root, repo, task_dir, accept):
    attempt = len(list(task_dir.glob("capture-*.json")))
    while True:
        attempt += 1
        capture = _capture(root, accept["base"]["vector"])
        candidates = _candidate_clocks(repo, accept["base"])
        for node in NODES:
            for frame in capture[node]["frames"]:
                candidates[node][frame["seq"]] = frame["clock"]
        cut = _greatest_feasible(candidates, accept["lower"])
        evidence = {
            "task": accept["id"],
            "attempt": attempt,
            "source": {"network": accept["network"], "branch": accept["branch"]},
            "accept_lower": accept["lower"],
            "base": accept["base"],
            "captured": {
                node: {
                    "upper": capture[node]["upper"],
                    "frames": [[f["seq"], f["clock"], _frame_digest(f["clock"], f["rows"])]
                               for f in capture[node]["frames"]],
                    "receipts": [[r["request"], r["digest"], r["seq"]]
                                 for r in capture[node]["receipts"]],
                } for node in NODES
            },
            "feasible": cut is not None,
            "cut": cut,
        }
        _write_json(task_dir / ("capture-%d.json" % attempt), evidence)
        if cut is not None:
            return capture, cut
        # No causally closed cut at or above the acceptance lower bound:
        # concurrent commits raced the capture. Re-capture; once writes stop,
        # the vector of current heads is always closed and reaches the bound.
        time.sleep(min(0.05 * attempt, 1.0))


def _publish_object(repo, task_id, payload):
    data = canonical(payload).encode("utf-8")
    sha = hashlib.sha256(data).hexdigest()
    tmp = repo / "objects" / ("tmp-%s-%d" % (task_id, os.getpid()))
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    _crash_point("object")
    # The payload bytes are already durable; only the rename plus the
    # liveness record happen inside the short metadata lock.
    with _FileLock(repo / "locks" / "gc.lock"):
        os.replace(tmp, repo / "objects" / sha)
        _fsync_dir(repo / "objects")
        state_path = repo / "tasks" / task_id / "state.json"
        state = _read_json(state_path) if state_path.exists() else {"held_objects": []}
        if sha not in state["held_objects"]:
            state["held_objects"].append(sha)
        _write_json(state_path, state)
    return sha


def _check_task_identity(record, identity, branch):
    if record.get("network") != identity["network"] or record.get("branch") != branch:
        raise OpsError("TASK_CONFLICT")


def cmd_backup(root, repo, task_id):
    _check_id(task_id)
    root = Path(root)
    if not root.is_dir():
        raise OpsError("ROOT_MISSING")
    repo = _ensure_repo(repo)
    identity = _read_source_identity(root)
    root_ops = _ensure_root_branch(root, identity["network"])
    branch = root_ops["branch"]
    task_dir = repo / "tasks" / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    with _FileLock(task_dir / "task.lock"):
        healed = None
        with _FileLock(repo / "locks" / "gc.lock"):
            retired = _load_retired(repo)
            if task_id in retired:
                raise OpsError("TASK_RETIRED")
            point_path = repo / "points" / (task_id + ".json")
            if point_path.exists():
                manifest = _read_json(point_path)
                _check_task_identity(manifest, identity, branch)
                healed = {"id": task_id, "network": manifest["network"],
                          "cut": manifest["cut"]}
                _write_json(task_dir / "commit.json", healed)
            else:
                accept_path = task_dir / "accept.json"
                if accept_path.exists():
                    accept = _read_json(accept_path)
                    _check_task_identity(accept, identity, branch)
                else:
                    base = _select_base(repo, branch, root_ops, retired)
                    accept = {
                        "id": task_id,
                        "network": identity["network"],
                        "branch": branch,
                        "lower": identity["heads"],
                        "base": base,
                        "floors": {node: base["vector"][node] + 1 for node in NODES},
                        "accepted_at": _now(),
                    }
                    # The acceptance record lands before any business payload is
                    # read or copied; it fixes the per-database head lower bound.
                    _write_json(accept_path, accept)
                    _crash_point("accept")
        if healed is not None:
            # A previous attempt may have died after committing but before
            # releasing the trim protection; heal that too.
            _release_protection(root, task_id)
            return healed
        _register_protection(root, task_id, accept["floors"])
        _slow()
        capture, cut = _capture_loop(root, repo, task_dir, accept)
        payload = {
            "format": REPO_FORMAT,
            "id": task_id,
            "network": accept["network"],
            "branch": accept["branch"],
            "base": accept["base"],
            "cut": cut,
            "frames": {node: capture[node]["frames"] for node in NODES},
            "receipts": {node: capture[node]["receipts"] for node in NODES},
        }
        sha = _publish_object(repo, task_id, payload)
        manifest = {
            "format": REPO_FORMAT,
            "id": task_id,
            "network": accept["network"],
            "branch": accept["branch"],
            "cut": cut,
            "capture_upper": {node: capture[node]["upper"] for node in NODES},
            "lower": accept["lower"],
            "base": accept["base"],
            "object": sha,
            "frame_index": {
                node: [[f["seq"], f["clock"], _frame_digest(f["clock"], f["rows"])]
                       for f in capture[node]["frames"]]
                for node in NODES
            },
            "receipt_index": {
                node: [[r["request"], r["digest"], r["seq"]]
                       for r in capture[node]["receipts"]]
                for node in NODES
            },
            "created": _now(),
        }
        _write_json(point_path, manifest)
        _crash_point("manifest")
        commit = {"id": task_id, "network": accept["network"], "cut": cut}
        _write_json(task_dir / "commit.json", commit)
        _release_protection(root, task_id)
        return commit


# ---------------------------------------------------------------------------
# Restore.

def _load_object(repo, sha):
    path = repo / "objects" / sha
    if not path.exists():
        raise OpsError("PAYLOAD_MISSING:" + sha[:16])
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != sha:
        raise OpsError("PAYLOAD_CORRUPT:" + sha[:16])
    return json.loads(data.decode("utf-8"))


def _assemble(chain, payloads):
    """Collect the cut frames and the branch's original receipts up to the cut.

    Each chain point contributes the range above its own base vector up to the
    vector its child used as a base (its capture upper bound on the same
    branch, or its own cut for a restored child branch), so post-cut frames
    and receipts of another branch are never carried over.
    """
    cut = chain[0]["cut"]
    limits = {chain[0]["id"]: cut}
    for i in range(len(chain) - 1):
        limits[chain[i + 1]["id"]] = chain[i]["base"]["vector"]
    frames = {node: {} for node in NODES}
    frame_digests = {node: {} for node in NODES}
    receipts = {node: {} for node in NODES}
    for manifest, payload in zip(chain, payloads):
        if payload.get("id") != manifest["id"] or payload.get("network") != manifest["network"]:
            raise OpsError("PAYLOAD_MISMATCH")
        base_v = manifest["base"]["vector"]
        limit = limits[manifest["id"]]
        for node in NODES:
            index = {seq: fd for seq, _clock, fd in manifest["frame_index"][node]}
            seen = set()
            for frame in payload["frames"][node]:
                fd = _frame_digest(frame["clock"], frame["rows"])
                if index.get(frame["seq"]) != fd:
                    raise OpsError("PAYLOAD_MISMATCH")
                seen.add(frame["seq"])
                if base_v[node] < frame["seq"] <= limit[node]:
                    old = frame_digests[node].get(frame["seq"])
                    if old is not None and old != fd:
                        raise OpsError("CHAIN_CONFLICT")
                    frames[node][frame["seq"]] = frame
                    frame_digests[node][frame["seq"]] = fd
            if seen != set(index):
                raise OpsError("PAYLOAD_MISMATCH")
            expected = {seq for _rq, _dg, seq in manifest["receipt_index"][node]}
            seen = set()
            for receipt in payload["receipts"][node]:
                seen.add(receipt["seq"])
                if base_v[node] < receipt["seq"] <= limit[node]:
                    if receipt["seq"] in receipts[node]:
                        raise OpsError("CHAIN_CONFLICT")
                    receipts[node][receipt["seq"]] = receipt
            if seen != expected:
                raise OpsError("PAYLOAD_MISMATCH")
    out = {}
    for node in NODES:
        seq = cut[node]
        frame = frames[node].get(seq)
        if frame is None:
            raise OpsError("CUT_FRAME_MISSING")
        if any(frame["clock"][other] > cut[other] for other in NODES):
            raise OpsError("CUT_NOT_CLOSED")
        if set(receipts[node]) != set(range(1, seq + 1)):
            raise OpsError("RECEIPTS_INCOMPLETE")
        out[node] = {
            "seq": seq,
            "clock": frame["clock"],
            "rows": frame["rows"],
            "receipts": [receipts[node][s] for s in range(1, seq + 1)],
        }
    return out


def _write_store_db(dest, node, network, assembled):
    dest = Path(dest)
    final = dest / (node + ".sqlite")
    part = dest / (node + ".sqlite.part")
    for suffix in (".sqlite.part", ".sqlite-wal", ".sqlite-shm", ".sqlite-journal"):
        stray = dest / (node + suffix)
        if stray.exists():
            stray.unlink()
    con = sqlite3.connect(str(part))
    try:
        con.executescript(STORE_DDL)
        con.execute("INSERT INTO identity VALUES(1,?,?,?)",
                    (network, node, assembled["seq"]))
        con.execute("INSERT INTO frames VALUES(?,?,?)",
                    (assembled["seq"], canonical(assembled["clock"]),
                     canonical(assembled["rows"])))
        con.executemany("INSERT INTO receipts VALUES(?,?,?)",
                        [(r["request"], r["digest"], r["seq"])
                         for r in assembled["receipts"]])
        con.commit()
    finally:
        con.close()
    with open(part, "rb") as fh:
        os.fsync(fh.fileno())
    os.replace(part, final)
    _fsync_dir(dest)


def _verify_dest(dest, manifest, assembled):
    from pipeline import close_pipeline, open_pipeline
    pipeline = open_pipeline(dest, manifest["network"])
    try:
        view = pipeline.view()
        for node in NODES:
            snap = view[node]
            if snap["seq"] != assembled[node]["seq"]:
                raise OpsError("VERIFY_FAILED:" + node)
            if snap["clock"] != assembled[node]["clock"]:
                raise OpsError("VERIFY_FAILED:" + node)
            if snap["rows"] != assembled[node]["rows"]:
                raise OpsError("VERIFY_FAILED:" + node)
            count = pipeline.stores[node].db.execute(
                "SELECT count(*) FROM receipts").fetchone()[0]
            if count != assembled[node]["seq"]:
                raise OpsError("VERIFY_FAILED:" + node)
    finally:
        close_pipeline(pipeline)


def _peek_network(dest):
    try:
        return json.loads((Path(dest) / "network.json").read_text(encoding="utf-8"))["network"]
    except (OSError, ValueError, KeyError):
        return None


def cmd_restore(repo, task_id, dest):
    _check_id(task_id)
    repo = _require_repo(repo)
    point_path = repo / "points" / (task_id + ".json")
    if not point_path.exists():
        raise OpsError("NO_SUCH_POINT")
    manifest = _read_json(point_path)
    dest = Path(dest)
    if dest.parent and not dest.parent.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
    lock_path = dest.parent / (dest.name + ".ops-restore.lock")
    with _FileLock(lock_path):
        marker_path = dest / MARKER
        token = None
        if dest.exists():
            if marker_path.exists():
                marker = _read_json(marker_path)
                if marker.get("point") != task_id:
                    raise OpsError("TARGET_BUSY")
                token = marker.get("token")
            elif _peek_network(dest) != manifest["network"]:
                # A pre-existing target not produced by this command is only
                # ever replaced after full verification, and only when it is
                # a business root of the same network.
                raise OpsError("TARGET_EXISTS")
        if token is None:
            token = "rs-" + uuid.uuid4().hex
        protection = repo / "restores" / (token + ".json")
        with _FileLock(repo / "locks" / "gc.lock"):
            if task_id in _load_retired(repo):
                raise OpsError("POINT_RETIRED")
            _write_json(protection, {"point": task_id, "dest": str(dest),
                                     "started": _now()})
        try:
            _slow()
            chain = _chain_manifests(repo, task_id)
            payloads = [_load_object(repo, m["object"]) for m in chain]
            assembled = _assemble(chain, payloads)
            # Corrupt or missing payloads are rejected here, before anything
            # is written into the target.
            _crash_point("restore-verify")
            dest.mkdir(parents=True, exist_ok=True)
            _write_json(marker_path, {"point": task_id, "token": token,
                                      "started": _now()})
            for node in NODES:
                _write_store_db(dest, node, manifest["network"], assembled[node])
            _write_json(dest / "network.json", {"network": manifest["network"]})
            branch = "br-" + hashlib.sha256(
                (token + ":" + task_id).encode("utf-8")).hexdigest()[:24]
            _write_json(dest / "ops.json", {
                "branch": branch,
                "network": manifest["network"],
                "base": {"point": task_id, "vector": manifest["cut"]},
                "restored_at": _now(),
            })
            stale_protect = dest / "ops-protect.json"
            if stale_protect.exists():
                stale_protect.unlink()
            _verify_dest(dest, manifest, assembled)
            with _FileLock(repo / "locks" / "gc.lock"):
                _write_json(repo / "roots" / (branch + ".json"),
                            {"point": task_id, "branch": branch, "created": _now()})
            _crash_point("restore-write")
            marker_path.unlink()
        finally:
            try:
                protection.unlink()
            except OSError:
                pass
    return {"id": task_id, "root": str(dest), "cut": manifest["cut"]}


# ---------------------------------------------------------------------------
# Prune: retire committed points and reclaim unreferenced payload objects.

def _live_objects(repo, retired):
    manifests = {}
    for path in (repo / "points").glob("*.json"):
        manifest = _read_json(path)
        manifests[manifest["id"]] = manifest
    live = set()

    def add_chain(point_id):
        seen = set()
        pid = point_id
        while pid is not None and pid not in seen:
            seen.add(pid)
            manifest = manifests.get(pid)
            if manifest is None:
                break
            live.add(manifest["object"])
            pid = manifest["base"]["point"]

    for manifest in manifests.values():
        if manifest["id"] not in retired:
            add_chain(manifest["id"])
    tasks = repo / "tasks"
    if tasks.is_dir():
        for task_dir in tasks.iterdir():
            if not task_dir.is_dir() or (task_dir / "commit.json").exists():
                continue
            accept_path = task_dir / "accept.json"
            if accept_path.exists():
                try:
                    base = _read_json(accept_path).get("base") or {}
                    if base.get("point"):
                        add_chain(base["point"])
                except (ValueError, OSError):
                    pass
            state_path = task_dir / "state.json"
            if state_path.exists():
                try:
                    live.update(_read_json(state_path).get("held_objects", []))
                except (ValueError, OSError):
                    pass
    for pattern in ("restores", "roots"):
        directory = repo / pattern
        if not directory.is_dir():
            continue
        for path in directory.glob("*.json"):
            try:
                add_chain(_read_json(path)["point"])
            except (ValueError, OSError, KeyError):
                pass
    return live


def cmd_prune(repo, drop_path):
    repo = _require_repo(repo)
    try:
        drop = json.loads(Path(drop_path).read_text(encoding="utf-8"))
    except ValueError:
        raise OpsError("BAD_DROP")
    if not isinstance(drop, list) or any(not isinstance(i, str) for i in drop):
        raise OpsError("BAD_DROP")
    for task_id in drop:
        _check_id(task_id)
    with _FileLock(repo / "locks" / "gc.lock"):
        retired = _load_retired(repo)
        for task_id in drop:
            if not (repo / "points" / (task_id + ".json")).exists():
                raise OpsError("UNKNOWN_POINT:" + task_id)
        retired |= set(drop)
        _store_retired(repo, retired)
        live = _live_objects(repo, retired)
        victims = []
        for path in (repo / "objects").iterdir():
            if path.name.startswith("tmp-") or path.name not in live:
                try:
                    victims.append((path, path.stat().st_size))
                except OSError:
                    pass
    _crash_point("prune-delete")
    reclaimed = 0
    for path, size in victims:
        try:
            path.unlink()
            reclaimed += size
        except FileNotFoundError:
            pass
    return {"retired": sorted(drop), "reclaimed_bytes": reclaimed}


# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(prog="ops")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("build")
    p.add_argument("out")
    p = sub.add_parser("backup")
    p.add_argument("root")
    p.add_argument("repo")
    p.add_argument("id")
    p = sub.add_parser("restore")
    p.add_argument("repo")
    p.add_argument("id")
    p.add_argument("dest")
    p = sub.add_parser("prune")
    p.add_argument("repo")
    p.add_argument("drop")
    args = parser.parse_args()
    try:
        if args.command == "build":
            result = cmd_build(args.out)
        elif args.command == "backup":
            result = cmd_backup(args.root, args.repo, args.id)
        elif args.command == "restore":
            result = cmd_restore(args.repo, args.id, args.dest)
        else:
            result = cmd_prune(args.repo, args.drop)
    except OpsError as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        raise SystemExit(2)
    except (ValueError, OSError, sqlite3.Error) as error:
        print(json.dumps({"error": str(error) or type(error).__name__},
                         ensure_ascii=False))
        raise SystemExit(2)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
