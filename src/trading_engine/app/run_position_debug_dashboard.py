from __future__ import annotations

import argparse

from trading_engine.debug.server import run as run_dashboard


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the position debug dashboard")
    parser.parse_args(argv)
    run_dashboard()


if __name__ == "__main__":
    run()
