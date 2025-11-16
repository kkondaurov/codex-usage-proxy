# Codex Usage Proxy

`codex-usage-proxy` is a local HTTP proxy plus terminal UI that mirrors the OpenAI API, forwards every request upstream, and records metered usage in SQLite so you can watch spend in real time.

**HIGHLY EXPERIMENTAL! NO GUARANTEES OF ACCURACY OR STABILITY! USE AT YOUR OWN RISK!**

## Codex CLI Integration

Codex ships with an “OpenAI” model provider that discovers its base URL via the `OPENAI_BASE_URL` environment variable. To interpose the proxy without breaking Codex defaults, run the proxy with its own upstream override variable and leave `OPENAI_BASE_URL` for Codex.

1. Start the proxy with its upstream target: defaults to `https://api.openai.com/v1`, but can be overriden via `CODEX_USAGE_UPSTREAM_BASE_URL` if you need Azure/OpenAI Europe, etc.:

   ```bash
   export CODEX_USAGE_UPSTREAM_BASE_URL="https://api.openai.com/v1"
   cargo run --release
   ```

2. In the shell where you run Codex CLI, point it at the proxy while keeping your API key scoped to the provider entry:

   ```bash
   export OPENAI_BASE_URL="http://127.0.0.1:8787/v1"
   codex chat   # or codex responses
   ```

3. Ensure the proxy’s `[server].public_base_path` stays `/v1` (the default) so Codex routes `/v1/responses` and `/v1/chat/completions` through the proxy. The proxy records both wire APIs; streaming responses are tapped to capture final usage tokens.

### Provider Snippet

Add the following block to `~/.codex/config.toml` so Codex knows how to call the proxy:

```toml
model_provider = "openai-via-proxy"

[model_providers.openai-via-proxy]
name = "OpenAI via proxy"
base_url = "http://127.0.0.1:8787/v1"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
```

- `wire_api = "responses"` keeps Codex on the Responses API. If you switch to `chat_completions`, the proxy will still forward traffic because it mirrors all `/v1/*` paths.
- Codex fetches `OPENAI_API_KEY` from your environment and hands it to the proxy, which forwards it upstream unchanged. The proxy never logs or stores the key.

## Config Tips

- Edit `codex-usage.toml` (or copy `codex-usage.example.toml`) to tweak listener ports, public paths, storage, and pricing.
- Environment overrides use `CODEX_USAGE_*` prefixes to avoid clashing with Codex: use `CODEX_USAGE_UPSTREAM_BASE_URL` for upstream changes, `CODEX_USAGE_LISTEN_ADDR` for the local socket, and `CODEX_USAGE_DB_PATH` to move the SQLite file.
- When experimenting, launch with `RUST_LOG=debug cargo run` to see proxy/aggregator logs in addition to the TUI.
