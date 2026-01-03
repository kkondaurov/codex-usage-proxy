# Codex Usage Tracker Architecture

## Overview

`codex-usage-tracker` is a single Rust binary that runs two major subsystems on the same Tokio runtime:

1. **Session Ingestor** – Watches the Codex session logs under `~/.codex/sessions/**/**/*.jsonl`, tails new lines, and extracts token usage deltas from `token_count` events.
2. **Terminal UI (TUI)** – Renders live usage stats in the terminal: rolling daily/weekly/monthly totals plus a table of the most recent sessions.

Both subsystems share a lightweight storage layer for persistence and cost calculation.

## Components

- **Session Ingestor (`src/ingest.rs`)**
  - Scans the session log tree and processes appended JSONL events.
  - Tracks per-file offsets and last-seen totals in SQLite (`ingest_state`).
  - Extracts per-turn token deltas from `token_count.info.total_token_usage`, attributing them to the current `turn_context.model`.
  - Updates session metadata (title, last summary, repo info) as it appears.

- **Storage (`src/storage/`)**
  - Wraps SQLite (default file `usage.db` beside the binary).
  - Core tables:
    - `sessions` – session metadata and lifetime token totals.
    - `session_turns` – per-turn token deltas (model-specific) with timestamps.
    - `session_daily_stats` – per-day per-session per-model aggregates.
    - `daily_stats` – per-day per-model aggregates.
    - `prices` – pricing rules (model prefix + effective date) populated from the remote dataset.
    - `ingest_state` – file offsets and last-seen totals for incremental parsing.
  - Costs are computed at read time via SQL joins; missing prices surface as `unknown` in the UI.

- **Configuration Layer (`src/config/`)**
  - Loads `codex-usage.toml` from the working directory (override via `--config`).
  - Fields: session log root, polling interval, SQLite path, display settings, pricing sync.
  - Environment variables can override matching fields.

- **Terminal UI (`src/tui/`)**
  - Implemented with `ratatui` + `crossterm`.
  - Layout: top summary block (last 10m / last hour / today) and bottom scrollable table of recent sessions.
  - Top Spending view ranks sessions by cost in the selected time window.
  - Stats view shows hourly/daily/weekly/monthly/yearly aggregates.
  - Pricing view shows the remote price table and sync status.

## Data Flow

1. Codex CLI writes JSONL session logs under `~/.codex/sessions/YYYY/MM/DD/`.
2. The ingestor tails new lines and extracts token deltas from `token_count` events.
3. Per-turn usage is stored in SQLite, along with session metadata and daily aggregates.
4. The TUI queries SQLite for live totals and renders updated views.

## Security & Privacy Notes

- No API keys are stored; the ingestor only reads local session logs.
- Request/response bodies are not persisted; only metadata (model, token counts) is stored.
- Debug logging is opt-in via `RUST_LOG`.
