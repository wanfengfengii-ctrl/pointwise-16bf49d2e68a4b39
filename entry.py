"""Dispatch a packaged or source-tree invocation to a named project component."""
import importlib
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
