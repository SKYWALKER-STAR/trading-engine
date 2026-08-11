from __future__ import annotations

from trading_engine import __main__
from trading_engine.app.engine_registry import EngineSpec


def test_main_routes_engine_args(monkeypatch: object) -> None:
    captured: dict[str, list[str] | None] = {"args": None}

    def fake_runner(argv: list[str] | None) -> None:
        captured["args"] = argv

    fake_specs = {
        "strategy": EngineSpec(
            name="strategy",
            description="strategy",
            runner=fake_runner,
        )
    }
    monkeypatch.setattr(__main__, "get_engine_specs", lambda: fake_specs)

    exit_code = __main__.main(["strategy", "--", "--once", "--symbol", "BTCUSDT"])

    assert exit_code == 0
    assert captured["args"] == ["--once", "--symbol", "BTCUSDT"]


def test_main_lists_engines(monkeypatch: object, capsys: object) -> None:
    fake_specs = {
        "position": EngineSpec(name="position", description="position", runner=lambda _argv: None),
        "strategy": EngineSpec(name="strategy", description="strategy", runner=lambda _argv: None),
    }
    monkeypatch.setattr(__main__, "get_engine_specs", lambda: fake_specs)

    exit_code = __main__.main(["--list-engines"])
    out = capsys.readouterr().out

    assert exit_code == 0
    assert "Available engines" in out
    assert "strategy" in out
    assert "position" in out


def test_main_requires_engine(monkeypatch: object, capsys: object) -> None:
    fake_specs = {
        "strategy": EngineSpec(name="strategy", description="strategy", runner=lambda _argv: None),
    }
    monkeypatch.setattr(__main__, "get_engine_specs", lambda: fake_specs)

    exit_code = __main__.main([])
    out = capsys.readouterr().out

    assert exit_code == 2
    assert "Available engines" in out