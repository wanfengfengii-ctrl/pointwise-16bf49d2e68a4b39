import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
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


PY = sys.executable
HERE = Path(__file__).resolve().parent
OPS = HERE / "ops.py"


def call(argv, cwd=None, env=None, ok=0, replace_env=False):
    full = {} if replace_env else dict(os.environ)
    full.pop("PYTHONPATH", None)
    if env:
        full.update(env)
    proc = subprocess.run([PY, "-B"] + [str(a) for a in argv],
                          capture_output=True, text=True, cwd=cwd, env=full)
    if ok is not None and proc.returncode != ok:
        raise AssertionError("argv=%r rc=%r\nstdout=%s\nstderr=%s"
                             % (argv, proc.returncode, proc.stdout, proc.stderr))
    return proc


def call_json(argv, **kw):
    return json.loads(call(argv, **kw).stdout)


def call_err(argv, **kw):
    proc = call(argv, ok=None, **kw)
    assert proc.returncode != 0, "expected failure: %r" % (argv,)
    return json.loads(proc.stdout)["error"]


def popen(argv, cwd=None, env=None):
    full = dict(os.environ)
    full.pop("PYTHONPATH", None)
    if env:
        full.update(env)
    return subprocess.Popen([PY, "-B"] + [str(a) for a in argv],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, cwd=cwd, env=full)


def wait_json(proc):
    out, err = proc.communicate()
    assert proc.returncode == 0, "rc=%r stderr=%s" % (proc.returncode, err)
    return json.loads(out)


def wait_deadline(predicate, what, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timeout waiting for " + what)


def ops_regression():
    import ops

    tmp = Path(tempfile.mkdtemp(prefix="water-ops-regression-"))
    evidence = {}
    try:
        # Cut selection: the interleaving documented in this file — raw=3 was
        # captured, then raw=4, clean=3, daily=3 committed with daily=3
        # depending on raw=4. (3,3,3) is complete per database but not closed.
        cands = {
            "raw": {3: {"raw": 3, "clean": 2, "daily": 2}},
            "clean": {3: {"raw": 4, "clean": 3, "daily": 2}},
            "daily": {3: {"raw": 4, "clean": 3, "daily": 3}},
        }
        zero = {"raw": 0, "clean": 0, "daily": 0}
        assert ops._greatest_feasible(cands, zero) is None
        cands["raw"][4] = {"raw": 4, "clean": 2, "daily": 2}
        assert ops._greatest_feasible(cands, zero) == {"raw": 4, "clean": 3, "daily": 3}
        assert ops._greatest_feasible(cands, {"raw": 5, "clean": 0, "daily": 0}) is None
        evidence["cut_selection"] = "nonclosed (3,3,3) rejected; (4,3,3) is the " \
            "component-wise maximum; acceptance lower bound enforced"

        # Reproducible, self-hosting release package.
        pkg1, pkg2, pkg3 = tmp / "pkg1", tmp / "pkg2", tmp / "pkg3"
        b1 = call_json([OPS, "build", pkg1])
        b2 = call_json([OPS, "build", pkg2])
        assert b1["sha256"] == b2["sha256"]
        b3 = call_json([pkg1, "ops", "build", pkg3], cwd=tmp)
        assert b3["sha256"] == b1["sha256"]
        evidence["package_sha256"] = b1["sha256"]

        repo = tmp / "repo"
        work = tmp / "work"
        root = work / "water"
        samples = HERE / "samples"
        call_json([pkg1, "run", "init", root, "sample-water"])
        call_json([pkg1, "run", "load", root, "raw-1", samples / "initial.json"])
        call_json([pkg1, "run", "clean", root, "clean-1", "2"])
        call_json([pkg1, "run", "daily", root, "daily-1"])
        call_json([pkg1, "run", "load", root, "raw-2", samples / "correction.json"])
        call_json([pkg1, "run", "clean", root, "clean-2", "2"])
        call_json([pkg1, "run", "daily", root, "daily-2"])
        call_json([pkg1, "run", "ack", root, "review-2"])
        day1 = call_json([pkg1, "run", "read", root])
        assert day1["raw"]["clock"] == {"raw": 3, "clean": 2, "daily": 2}

        p1 = call_json([pkg1, "ops", "backup", root, repo, "p1"])
        assert p1["cut"] == {"raw": 3, "clean": 2, "daily": 2}
        assert call_json([pkg1, "ops", "backup", root, repo, "p1"]) == p1

        batch3 = tmp / "batch3.json"
        batch3.write_text(json.dumps([dict(INITIAL[2], rev=3, value=8)]),
                          encoding="utf-8")
        call_json([pkg1, "run", "load", root, "raw-3", batch3])
        call_json([pkg1, "run", "clean", root, "clean-3", "2"])
        call_json([pkg1, "run", "daily", root, "daily-3"])
        call_json([pkg1, "run", "ack", root, "review-3"])
        call_json([pkg1, "run", "trim", root, "raw", "1"])
        p2 = call_json([pkg1, "ops", "backup", root, repo, "p2"])
        assert p2["cut"] == {"raw": 5, "clean": 3, "daily": 3}
        expected_p2 = call_json([pkg1, "run", "read", root])

        # Incremental discipline: p2 read only frames/receipts above p1's
        # capture upper bound; p1 (first backup) holds all retained history.
        m2 = json.loads((repo / "points" / "p2.json").read_text())
        assert m2["base"] == {"point": "p1", "vector": p1["cut"]}
        payload2 = json.loads((repo / "objects" / m2["object"]).read_text())
        assert [f["seq"] for f in payload2["frames"]["raw"]] == [5]
        assert [f["seq"] for f in payload2["frames"]["clean"]] == [3]
        assert [r["seq"] for r in payload2["receipts"]["raw"]] == [4, 5]
        m1 = json.loads((repo / "points" / "p1.json").read_text())
        payload1 = json.loads((repo / "objects" / m1["object"]).read_text())
        assert [f["seq"] for f in payload1["frames"]["raw"]] == [0, 1, 2, 3]
        assert [r["seq"] for r in payload1["receipts"]["raw"]] == [1, 2, 3]
        evidence["increment"] = {"p1_raw_frames": [0, 1, 2, 3],
                                 "p2_raw_frames": [5], "p2_base": "p1"}

        # Divergent commits on the original branch after p2.
        batchx = tmp / "batchx.json"
        batchx.write_text(json.dumps(
            [{"id": "x1", "rev": 1, "station": "north", "day": 5, "value": 3,
              "deleted": False}]), encoding="utf-8")
        call_json([pkg1, "run", "load", root, "raw-x", batchx])
        call_json([pkg1, "run", "clean", root, "clean-x", "2"])
        call_json([pkg1, "run", "daily", root, "daily-x"])
        p2b = call_json([pkg1, "ops", "backup", root, repo, "p2b"])
        assert p2b["cut"] == {"raw": 6, "clean": 4, "daily": 4}
        expected_p2b = call_json([pkg1, "run", "read", root])

        # Trim respects the history protection of an accepted backup task and
        # deletes again once the task commits.
        def clean_frames():
            con = sqlite3.connect(str(root / "clean.sqlite"))
            try:
                return [r[0] for r in con.execute("SELECT seq FROM frames ORDER BY seq")]
            finally:
                con.close()

        for req in ("clean-p1", "clean-p2", "clean-p3"):
            call_json([pkg1, "run", "clean", root, req, "2"])
        prot = popen([pkg1, "ops", "backup", root, repo, "prot1"],
                     env={"OPS_SLOW_MS": "1500"})
        wait_deadline(lambda: "prot1" in json.loads(
            (root / "ops-protect.json").read_text())["tasks"], "prot1 protection")
        call_json([pkg1, "run", "trim", root, "clean", "1"])
        assert clean_frames() == [5, 6, 7], clean_frames()
        prot1 = wait_json(prot)
        assert prot1["cut"] == {"raw": 6, "clean": 7, "daily": 4}
        call_json([pkg1, "run", "trim", root, "clean", "1"])
        assert clean_frames() == [7], clean_frames()
        evidence["trim_protection"] = "frames 5-7 survived trim during an " \
            "accepted backup; keep=1 retention applied after commit"

        # Real SIGKILL at persistence boundaries; retries finish the tasks.
        dead = call([pkg1, "ops", "backup", root, repo, "k1"],
                    env={"OPS_CRASH_AT": "object"}, ok=None)
        assert dead.returncode == -signal.SIGKILL
        assert list((repo / "objects").glob("tmp-k1-*"))
        k1 = call_json([pkg1, "ops", "backup", root, repo, "k1"])
        assert k1["cut"] == prot1["cut"]

        dead = call([pkg1, "ops", "backup", root, repo, "k2"],
                    env={"OPS_CRASH_AT": "accept"}, ok=None)
        assert dead.returncode == -signal.SIGKILL
        accept_k2 = json.loads((repo / "tasks" / "k2" / "accept.json").read_text())
        k2 = call_json([pkg1, "ops", "backup", root, repo, "k2"])
        assert k2["cut"] == prot1["cut"]
        assert json.loads((repo / "tasks" / "k2" / "accept.json").read_text()) == accept_k2

        dead = call([pkg1, "ops", "backup", root, repo, "k3"],
                    env={"OPS_CRASH_AT": "manifest"}, ok=None)
        assert dead.returncode == -signal.SIGKILL
        k3 = call_json([pkg1, "ops", "backup", root, repo, "k3"])
        assert k3["cut"] == prot1["cut"]
        assert len(list((repo / "tasks" / "k3").glob("capture-*.json"))) == 1
        evidence["kill_tests"] = "SIGKILL at accept/object/manifest; tasks " \
            "resumed from persistent state and committed"

        # Different tasks run in parallel; one ID commits exactly one result.
        procs = [popen([pkg1, "ops", "backup", root, repo, name],
                       env={"OPS_SLOW_MS": "600"})
                 for name in ("c1", "c2", "dup", "dup")]
        results = [wait_json(p) for p in procs]
        assert results[2] == results[3]
        assert all(r["cut"] == prot1["cut"] for r in results)
        other = tmp / "other"
        call_json([pkg1, "run", "init", other, "other-net"])
        err = call_err([pkg1, "ops", "backup", other, repo, "dup"])
        evidence["concurrency"] = "c1/c2/dup ran in parallel; duplicate ID " \
            "returned one result; cross-network ID reuse rejected: " + err

        # A final source-branch point whose payload later proves reclamation.
        batche = tmp / "batche.json"
        batche.write_text(json.dumps(
            [{"id": "e1", "rev": 1, "station": "east", "day": 6, "value": 4,
              "deleted": False}]), encoding="utf-8")
        call_json([pkg1, "run", "load", root, "raw-end", batche])
        pend = call_json([pkg1, "ops", "backup", root, repo, "pend"])
        assert pend["cut"] == {"raw": 7, "clean": 7, "daily": 4}
        pend_obj = repo / "objects" / json.loads(
            (repo / "points" / "pend.json").read_text())["object"]
        pend_size = pend_obj.stat().st_size

        # Moving the source root does not change its persisted identity.
        moved = work / "water-moved"
        root.rename(moved)
        assert call_json([pkg1, "ops", "backup", moved, repo, "pend"]) == pend

        # Disaster: the source root and the source tree are gone. Only a copy
        # of the release package and the repository remain.
        iso = tmp / "isolated"
        iso.mkdir()
        shutil.copy(pkg1, iso / "pkg")
        shutil.rmtree(work)
        minimal = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}

        destR = tmp / "destR"
        r2 = call_json([iso / "pkg", "ops", "restore", repo, "p2", destR],
                       cwd=iso, env=minimal, replace_env=True)
        assert r2 == {"id": "p2", "root": str(destR), "cut": p2["cut"]}
        got = call_json([iso / "pkg", "run", "read", destR],
                        cwd=iso, env=minimal, replace_env=True)
        assert got == expected_p2
        evidence["disaster_restore"] = {"cut": r2["cut"],
                                        "read_matches_source": got == expected_p2}

        # Original receipts are replayed; conflicts still detected.
        assert call_json([pkg1, "run", "load", destR, "raw-1",
                          samples / "initial.json"])["seq"] == 1
        assert call_json([pkg1, "run", "clean", destR, "clean-1", "2"])["seq"] == 1
        assert call_json([pkg1, "run", "ack", destR, "review-3"])["seq"] == 5
        assert "REQUEST_CONFLICT" in call_err(
            [pkg1, "run", "clean", destR, "clean-1", "3"])

        # Processing continues on the restored branch; its incremental base
        # is the restore point itself, not the repository's latest point.
        batchr = tmp / "batchr.json"
        batchr.write_text(json.dumps(
            [{"id": "r1", "rev": 1, "station": "south", "day": 7, "value": 9,
              "deleted": False}]), encoding="utf-8")
        assert call_json([pkg1, "run", "load", destR, "raw-r", batchr])["seq"] == 6
        assert call_json([pkg1, "run", "clean", destR, "clean-r", "2"])["seq"] == 4
        assert call_json([pkg1, "run", "daily", destR, "daily-r"])["seq"] == 4
        destR_read = call_json([pkg1, "run", "read", destR])
        p3 = call_json([pkg1, "ops", "backup", destR, repo, "p3"])
        assert p3["cut"] == {"raw": 6, "clean": 4, "daily": 4}
        m3 = json.loads((repo / "points" / "p3.json").read_text())
        assert m3["base"] == {"point": "p2", "vector": p2["cut"]}
        evidence["restored_branch_base"] = m3["base"]

        # Same cut vector on two branches; receipts never cross branches.
        def receipt_map(root_path, node):
            con = sqlite3.connect(str(root_path / (node + ".sqlite")))
            try:
                return {r[0]: r[1] for r in con.execute("SELECT request,seq FROM receipts")}
            finally:
                con.close()

        destR3 = tmp / "destR3"
        call_json([pkg1, "ops", "restore", repo, "p3", destR3])
        assert call_json([pkg1, "run", "read", destR3]) == destR_read
        rr3 = receipt_map(destR3, "raw")
        assert rr3.get("raw-r") == 6 and "raw-x" not in rr3
        destB = tmp / "destB"
        call_json([pkg1, "ops", "restore", repo, "p2b", destB])
        assert call_json([pkg1, "run", "read", destB]) == expected_p2b
        rrb = receipt_map(destB, "raw")
        assert rrb.get("raw-x") == 6 and "raw-r" not in rrb
        evidence["branch_isolation"] = "p3 and p2b share cut (6,4,4) but keep " \
            "their own seq-6 receipts (raw-r vs raw-x)"

        # Corrupt or missing payloads are rejected before the target is touched.
        repoC = tmp / "repoC"
        shutil.copytree(repo, repoC)
        obj = repoC / "objects" / m2["object"]
        data = bytearray(obj.read_bytes())
        data[len(data) // 2] ^= 1
        obj.write_bytes(bytes(data))
        assert "PAYLOAD_CORRUPT" in call_err([pkg1, "ops", "restore", repoC, "p2",
                                              tmp / "destC"])
        assert not (tmp / "destC").exists()
        destGood = tmp / "destGood"
        call_json([pkg1, "ops", "restore", repo, "p1", destGood])
        before = call_json([pkg1, "run", "read", destGood])
        assert "PAYLOAD_CORRUPT" in call_err([pkg1, "ops", "restore", repoC, "p2",
                                              destGood])
        assert call_json([pkg1, "run", "read", destGood]) == before
        obj.unlink()
        assert "PAYLOAD_MISSING" in call_err([pkg1, "ops", "restore", repoC, "p2",
                                              tmp / "destC2"])
        evidence["corruption"] = "corrupt and missing payloads rejected before " \
            "target replacement; pre-existing target verified untouched"

        # Pruning: safe reclamation, ancestor preservation, in-flight restore
        # protection, real byte accounting, idempotence.
        drop = tmp / "drop.json"
        drop.write_text("[]")
        first = call_json([pkg1, "ops", "prune", repo, drop])
        assert first["retired"] == [] and first["reclaimed_bytes"] > 0
        drop.write_text(json.dumps(["p1"]))
        second = call_json([pkg1, "ops", "prune", repo, drop])
        assert second == {"retired": ["p1"], "reclaimed_bytes": 0}
        call_json([pkg1, "ops", "restore", repo, "p2", tmp / "destP2again"])
        drop.write_text(json.dumps(["p2"]))
        slow_restore = popen([pkg1, "ops", "restore", repo, "p2", tmp / "destRace"],
                             env={"OPS_SLOW_MS": "1500"})
        wait_deadline(lambda: list((repo / "restores").glob("*.json")),
                      "restore protection")
        third = call_json([pkg1, "ops", "prune", repo, drop])
        assert third["retired"] == ["p2"]
        raced = wait_json(slow_restore)
        assert raced["cut"] == p2["cut"]
        assert "POINT_RETIRED" in call_err([pkg1, "ops", "restore", repo, "p2",
                                            tmp / "destP2x"])
        assert "TASK_RETIRED" in call_err([pkg1, "ops", "backup", destR, repo, "p2"])
        drop.write_text(json.dumps(["pend"]))
        fourth = call_json([pkg1, "ops", "prune", repo, drop])
        assert fourth == {"retired": ["pend"], "reclaimed_bytes": pend_size}
        assert not pend_obj.exists()
        assert "POINT_RETIRED" in call_err([pkg1, "ops", "restore", repo, "pend",
                                            tmp / "destPe"])
        again = call_json([pkg1, "ops", "prune", repo, drop])
        assert again == {"retired": ["pend"], "reclaimed_bytes": 0}
        evidence["prune"] = {"orphan_reclaimed": first["reclaimed_bytes"],
                             "ancestor_retired_reclaimed": second["reclaimed_bytes"],
                             "unreferenced_reclaimed": fourth["reclaimed_bytes"],
                             "race": "restore accepted before retirement completed"}

        # Acceptance, capture, reference and success evidence is in the repo.
        accept2 = json.loads((repo / "tasks" / "p2" / "accept.json").read_text())
        assert accept2["lower"] == {"raw": 5, "clean": 3, "daily": 3}
        cap2 = json.loads((repo / "tasks" / "p2" / "capture-1.json").read_text())
        assert cap2["captured"]["raw"]["upper"] == 5
        assert cap2["accept_lower"] == accept2["lower"]
        state2 = json.loads((repo / "tasks" / "p2" / "state.json").read_text())
        assert m2["object"] in state2["held_objects"]
        assert json.loads((repo / "tasks" / "p2" / "commit.json").read_text())["cut"] == p2["cut"]

        final = call_json([pkg1, "ops", "restore", repo, "p3", tmp / "destFinal"])
        assert final["cut"] == p3["cut"]
        evidence["cuts"] = {"p1": p1["cut"], "p2": p2["cut"], "p2b": p2b["cut"],
                            "p3": p3["cut"]}
        print("OPS_REGRESSION_OK")
        print(json.dumps(evidence, ensure_ascii=False, sort_keys=True))
    except BaseException:
        print("OPS_REGRESSION_WORKSPACE_KEPT=" + str(tmp))
        raise
    else:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    normal()
    ops_regression()
