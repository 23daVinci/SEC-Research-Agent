"""Load the YAML config and environment overrides."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path("config/ingest.yml")


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    tickers: tuple[str, ...]
    eight_k_window_months: int
    rate_limit_per_sec: float
    max_attempts: int
    connect_timeout_s: float
    read_timeout_s: float
    bronze_root: Path
    user_agent: str | None = None


def load_config(path: Path | None = None, env: Mapping[str, str] | None = None) -> Config:
    """Read the YAML file; `BRONZE_ROOT` and `SEC_USER_AGENT` come from the environment.

    `SEC_INGEST_CONFIG` selects the file when `path` is not given. The user agent is optional here
    and enforced by `EdgarClient`, so offline steps (selection, validation) don't need it.
    """
    env = os.environ if env is None else env
    path = path or Path(env.get("SEC_INGEST_CONFIG", DEFAULT_CONFIG_PATH))
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as e:
        raise ConfigError(f"cannot read config {path}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"{path} must contain a mapping")

    try:
        tickers = tuple(str(t).upper() for t in raw["tickers"])
        cfg = Config(
            tickers=tickers,
            eight_k_window_months=int(raw["eight_k_window_months"]),
            rate_limit_per_sec=float(raw["rate_limit_per_sec"]),
            max_attempts=int(raw["max_attempts"]),
            connect_timeout_s=float(raw["connect_timeout_s"]),
            read_timeout_s=float(raw["read_timeout_s"]),
            bronze_root=Path(env.get("BRONZE_ROOT") or raw["bronze_root"]),
            user_agent=(env.get("SEC_USER_AGENT") or "").strip() or None,
        )
    except KeyError as e:
        raise ConfigError(f"missing config key {e}") from e

    if not cfg.tickers or len(set(cfg.tickers)) != len(cfg.tickers):
        raise ConfigError("tickers must be non-empty and unique")
    if cfg.rate_limit_per_sec <= 0 or cfg.rate_limit_per_sec > 10:
        raise ConfigError("rate_limit_per_sec must be in (0, 10]; SEC hard cap is 10")
    if cfg.max_attempts < 1:
        raise ConfigError("max_attempts must be >= 1")
    return cfg
