from pathlib import Path

import pytest

from sec_ingest.config import ConfigError, load_config

YAML = """\
tickers: [aapl, MSFT]
eight_k_window_months: 6
rate_limit_per_sec: 5
max_attempts: 3
connect_timeout_s: 10
read_timeout_s: 60
bronze_root: ./data
"""


def write(tmp_path: Path, text: str = YAML) -> Path:
    p = tmp_path / "ingest.yml"
    p.write_text(text)
    return p


def test_loads_and_normalizes(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path), env={})
    assert cfg.tickers == ("AAPL", "MSFT")
    assert cfg.bronze_root == Path("./data")
    assert cfg.user_agent is None


def test_env_overrides(tmp_path: Path) -> None:
    cfg = load_config(
        write(tmp_path), env={"BRONZE_ROOT": "/x", "SEC_USER_AGENT": "  Jane j@example.com "}
    )
    assert cfg.bronze_root == Path("/x")
    assert cfg.user_agent == "Jane j@example.com"


def test_missing_key(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="max_attempts"):
        load_config(write(tmp_path, YAML.replace("max_attempts: 3\n", "")), env={})


def test_rate_limit_above_sec_cap_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="rate_limit"):
        load_config(
            write(tmp_path, YAML.replace("rate_limit_per_sec: 5", "rate_limit_per_sec: 11")), env={}
        )


def test_duplicate_tickers_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unique"):
        load_config(write(tmp_path, YAML.replace("[aapl, MSFT]", "[AAPL, aapl]")), env={})


def test_universe_defaults_to_tickers_and_is_validated(tmp_path: Path) -> None:
    assert load_config(write(tmp_path), env={}).universe == "tickers"
    assert load_config(write(tmp_path, YAML + "universe: sp500\n"), env={}).universe == "sp500"
    with pytest.raises(ConfigError, match="universe"):
        load_config(write(tmp_path, YAML + "universe: nasdaq\n"), env={})
