from __future__ import annotations

from pathlib import Path

import pytest

from mgc_v05l.app.schwab_token_bootstrap_web import _probe_resolution


def test_probe_resolution_rejects_placeholder_quote_symbol(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHWAB_APP_KEY", "key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")

    config_path = tmp_path / "schwab.local.json"
    config_path.write_text(
        """
{
  "historical_symbol_map": {"MGC": "/MGC"},
  "quote_symbol_map": {"MGC": "REPLACE_WITH_CONFIRMED_SCHWAB_QUOTE_SYMBOL"},
  "timeframe_map": {"5m": {"frequency_type": "minute", "frequency": 5}}
}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Schwab config placeholder error"):
        _probe_resolution(token_file=None, schwab_config_path=config_path, probe_symbol="MGC")
