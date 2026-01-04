# Codex Usage Tracker

When using `codex-cli` with an OpenAI API key, there's no access to usage/spend data.

`codex-usage-tracker` monitors Codex usage in real time by reading the JSONL session logs that Codex writes under `~/.codex/sessions`.

Under the hood, it tails session files as they are updated and aggregates token usage + costs in SQLite for a live terminal UI.

## Screenshots
<details>

<summary>Overview</summary>

![overview](/screenshots/1-overview-v5.png)

</details>


<details>

<summary>Session details</summary>

![overview](/screenshots/2-sessions-v4.png)

</details>

<details>

<summary>Stats per hour, day, week, month and year</summary>

![overview](/screenshots/3-stats-v4.png)

</details>

<details>

<summary>Pricing configuration</summary>

![overview](/screenshots/4-pricing-v4.png)

</details>


## Quickstart

[Install `rust` and `cargo`](https://doc.rust-lang.org/cargo/getting-started/installation.html)

```
curl https://sh.rustup.rs -sSf | sh
```

Copy the config:
```
cp codex-usage.example.toml codex-usage.toml
```

Build and start the tracker:

```
cargo run --release
```

Run `codex` normally. The tracker will pick up session logs from `~/.codex/sessions` as they are written.

Pricing is fetched from the remote pricing dataset on first run and refreshed periodically. Use the Pricing tab (`4`) in the TUI to view the current price table and press `R` to refresh manually.

To rebuild usage data from logs (clear non-pricing tables first):
```
cargo run --release -- --rebuild
```

## Inspiration

This project’s “overview” view was inspired by [`codex-wrapped`](https://github.com/numman-ali/codex-wrapped).
