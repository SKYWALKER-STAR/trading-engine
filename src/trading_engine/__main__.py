from __future__ import annotations

import argparse
import sys

from trading_engine.app.engine_registry import EngineSpec, get_engine_specs


def build_parser(engine_names: list[str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m trading_engine",
        description="Run a specific trading engine process",
    )
    parser.add_argument(
        "engine",
        nargs="?",
        choices=engine_names,
        help="Engine name to run",
    )
    parser.add_argument(
        "engine_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to the selected engine",
    )
    parser.add_argument(
        "--list-engines",
        action="store_true",
        help="List available engine names and exit",
    )
    return parser


def _print_engine_list(specs: dict[str, EngineSpec]) -> None:
    print("Available engines:")
    for name in sorted(specs):
        print(f"- {name}: {specs[name].description}")


def main(argv: list[str] | None = None) -> int:
    specs = get_engine_specs()
    engine_names = sorted(specs.keys())
    parser = build_parser(engine_names)
    args = parser.parse_args(argv)

    if args.list_engines:
        _print_engine_list(specs)
        return 0

    if args.engine is None:
        parser.print_help()
        print()
        _print_engine_list(specs)
        return 2

    spec = specs[args.engine]
    passthrough_args = list(args.engine_args)
    if passthrough_args and passthrough_args[0] == "--":
        passthrough_args = passthrough_args[1:]

    spec.runner(passthrough_args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
