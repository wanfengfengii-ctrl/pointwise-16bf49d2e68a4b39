import argparse
import json
from pathlib import Path
from pipeline import NODES, close_pipeline, open_pipeline
from samples import INITIAL, CORRECTION


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("init")
    p.add_argument("root")
    p.add_argument("network")
    p = sub.add_parser("load")
    p.add_argument("root")
    p.add_argument("request")
    p.add_argument("input")
    p = sub.add_parser("clean")
    p.add_argument("root")
    p.add_argument("request")
    p.add_argument("factor", type=int)
    p = sub.add_parser("daily")
    p.add_argument("root")
    p.add_argument("request")
    p = sub.add_parser("ack")
    p.add_argument("root")
    p.add_argument("request")
    p = sub.add_parser("read")
    p.add_argument("root")
    p = sub.add_parser("trim")
    p.add_argument("root")
    p.add_argument("node", choices=NODES)
    p.add_argument("keep", type=int)
    sub.add_parser("demo")
    args = parser.parse_args()
    if args.command == "demo":
        import sqlite3
        from pipeline import Pipeline, Store
        pipeline = Pipeline(
            {n: Store(sqlite3.connect(":memory:"), n, "sample-water") for n in NODES})
    else:
        root = Path(args.root)
        if args.command == "init":
            if root.exists():
                raise ValueError("ROOT_EXISTS")
            if not args.network:
                raise ValueError("BAD_IDENTITY")
            root.mkdir(parents=True)
            (root / "network.json").write_text(
                json.dumps({"network": args.network}), encoding="utf-8")
        pipeline = open_pipeline(root, args.network if args.command == "init" else None)
    try:
        if args.command == "init":
            result = pipeline.view()
        elif args.command == "load":
            batch = json.loads(Path(args.input).read_text(encoding="utf-8"))
            result = pipeline.load(args.request, batch)
        elif args.command == "clean":
            result = pipeline.clean(args.request, args.factor)
        elif args.command == "daily":
            result = pipeline.daily(args.request)
        elif args.command == "ack":
            result = pipeline.acknowledge(args.request)
        elif args.command == "read":
            result = pipeline.view()
        elif args.command == "trim":
            pipeline.stores[args.node].trim(args.keep)
            result = pipeline.stores[args.node].snapshot()
        else:
            pipeline.load("raw-1", INITIAL)
            pipeline.clean("clean-1", 2)
            pipeline.daily("daily-1")
            before = pipeline.view()
            pipeline.load("raw-2", CORRECTION)
            pipeline.clean("clean-2", 2)
            pipeline.daily("daily-2")
            result = {"before": before, "after": pipeline.view()}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    finally:
        close_pipeline(pipeline)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        raise SystemExit(2)
