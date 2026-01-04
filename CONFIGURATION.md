# Configuration Reference

This document defines the local configuration format for `codex-dashboard`, including where the config lives, how each key behaves, and how pricing sync works.

## File Locations

- The binary searches for `codex-usage.toml` in the current working directory. Override with `--config /path/to/file.toml`.
- Keep secrets (like `OPENAI_API_KEY`) in your shell environment; the config never stores keys.
- Example config: `codex-usage.example.toml` in the repo root. Copy it next to the binary and edit as needed.

## Top-Level Table Layout

```toml
[storage]
database_path = "usage.db"
flush_interval_secs = 5

[display]
recent_events_capacity = 500
refresh_hz = 10

[sessions]
# root_dir defaults to ~/.codex/sessions if omitted.
root_dir = "/Users/you/.codex/sessions"
poll_interval_secs = 2

[pricing]
currency = "USD"

[pricing.remote]
url = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
refresh_interval_hours = 24
timeout_secs = 5

[alerts]
# Optional budget thresholds (USD) for visual warnings.
# daily_budget_usd = 50.0
# monthly_budget_usd = 500.0
```

### Sections

| Section | Purpose | Notes |
| --- | --- | --- |
| `[storage]` | SQLite file location and sync settings | `flush_interval_secs` controls how often aggregates are forced to disk. |
| `[display]` | TUI presentation knobs | Increase `recent_events_capacity` if you want a longer history in the table. |
| `[sessions]` | Session log ingestion | `root_dir` points at Codex session logs; `poll_interval_secs` controls scan cadence. |
| `[pricing]` | Currency + pricing sync | Currency is informational only. Prices are fetched from the remote dataset and stored locally. |
| `[pricing.remote]` | Remote pricing settings | `url` points at the pricing dataset, `refresh_interval_hours` controls background refresh, `timeout_secs` limits fetch time. |
| `[alerts]` | Optional budget thresholds | `daily_budget_usd` and `monthly_budget_usd` drive warning highlights in the UI when exceeded. |

Environment overrides:

| Env var | Overrides |
| --- | --- |
| `CODEX_USAGE_DB_PATH` | `[storage].database_path` |
| `CODEX_USAGE_SESSIONS_DIR` | `[sessions].root_dir` |
| `CODEX_USAGE_SESSIONS_POLL_INTERVAL_SECS` | `[sessions].poll_interval_secs` |

## Remote Pricing

Pricing is pulled from the remote dataset and cached in SQLite. On each refresh, the local `prices` table is replaced with the latest dataset and applied to all historical usage. If a model is missing from the dataset, costs display as `unknown` until the next successful sync.
