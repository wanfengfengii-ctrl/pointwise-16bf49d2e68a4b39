"""Go-live demo: build the release package, run a business day, back up,
lose the source root and the source tree, restore from the package copy and
the backup repository only, and continue processing on the restored branch.

Every printed block is the real output of the packaged commands.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = sys.executable


def call(argv, cwd=None):
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    proc = subprocess.run([PY, "-B"] + [str(a) for a in argv],
                          capture_output=True, text=True, cwd=cwd, env=env)
    if proc.returncode != 0:
        raise SystemExit("FAILED %s\nstdout=%s\nstderr=%s"
                         % (argv, proc.stdout, proc.stderr))
    return json.loads(proc.stdout)


def show(title, value):
    print("\n=== %s ===" % title)
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def main():
    tmp = Path(tempfile.mkdtemp(prefix="water-demo-"))
    try:
        pkg = tmp / "water-ops.pyz"
        build = call([HERE / "ops.py", "build", pkg])
        show("1. reproducible release package", build)

        root = tmp / "work" / "water"
        repo = tmp / "repo"
        samples = HERE / "samples"
        call([pkg, "run", "init", root, "sample-water"])
        call([pkg, "run", "load", root, "raw-1", samples / "initial.json"])
        call([pkg, "run", "clean", root, "clean-1", "2"])
        call([pkg, "run", "daily", root, "daily-1"])
        call([pkg, "run", "load", root, "raw-2", samples / "correction.json"])
        call([pkg, "run", "clean", root, "clean-2", "2"])
        call([pkg, "run", "daily", root, "daily-2"])
        call([pkg, "run", "ack", root, "review-2"])
        day1 = call([pkg, "ops", "backup", root, repo, "day1"])
        show("2. first backup (full history and receipts)", day1)

        batch = tmp / "batch3.json"
        batch.write_text(json.dumps(
            [{"id": "c", "rev": 3, "station": "east", "day": 0, "value": 8,
              "deleted": False}]), encoding="utf-8")
        call([pkg, "run", "load", root, "raw-3", batch])
        call([pkg, "run", "clean", root, "clean-3", "2"])
        call([pkg, "run", "daily", root, "daily-3"])
        call([pkg, "run", "ack", root, "review-3"])
        call([pkg, "run", "trim", root, "raw", "1"])
        day2 = call([pkg, "ops", "backup", root, repo, "day2"])
        show("3. incremental backup after trim", day2)

        station = tmp / "station"
        station.mkdir()
        shutil.copy(pkg, station / "water-ops.pyz")
        shutil.rmtree(tmp / "work")
        show("4. disaster", {
            "source_root": str(tmp / "work") + " (deleted)",
            "source_tree": "not used from here on",
            "remaining": ["release package copy", "backup repository"],
        })

        restored = station / "restored"
        r2 = call([station / "water-ops.pyz", "ops", "restore", repo, "day2", restored],
                  cwd=station)
        show("5. restore from package + repository only", r2)
        view = call([station / "water-ops.pyz", "run", "read", restored], cwd=station)
        show("6. restored state (python3 -B water-ops.pyz run read)", view)

        replay = call([station / "water-ops.pyz", "run", "load", restored,
                       "raw-1", samples / "initial.json"], cwd=station)
        show("7. old request returns its original receipt", replay)

        batch2 = station / "new-batch.json"
        batch2.write_text(json.dumps(
            [{"id": "n1", "rev": 1, "station": "north", "day": 1, "value": 4,
              "deleted": False}]), encoding="utf-8")
        cont = [
            call([station / "water-ops.pyz", "run", "load", restored, "raw-4", batch2],
                 cwd=station),
            call([station / "water-ops.pyz", "run", "clean", restored, "clean-4", "2"],
                 cwd=station),
            call([station / "water-ops.pyz", "run", "daily", restored, "daily-4"],
                 cwd=station),
        ]
        show("8. processing continues on the restored branch", cont)
        day3 = call([station / "water-ops.pyz", "ops", "backup", restored, repo, "day3"],
                    cwd=station)
        show("9. incremental backup of the restored branch", day3)

        drop = station / "drop.json"
        drop.write_text(json.dumps(["day1"]))
        pruned = call([station / "water-ops.pyz", "ops", "prune", repo, drop],
                      cwd=station)
        show("10. retire day1 (descendants' ancestor payloads preserved)", pruned)
        err = subprocess.run(
            [PY, "-B", str(station / "water-ops.pyz"), "ops", "backup",
             restored, repo, "day1"],
            capture_output=True, text=True, cwd=station)
        show("11. retired ID rejected for backup", json.loads(err.stdout))

        show("summary", {
            "package_sha256": build["sha256"],
            "cuts": {"day1": day1["cut"], "day2": day2["cut"], "day3": day3["cut"]},
            "restored_cut": r2["cut"],
            "receipt_replay": replay,
            "prune": pruned,
        })
    finally:
        if os.environ.get("DEMO_KEEP"):
            print("\ndemo workspace kept: %s" % tmp)
        else:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
