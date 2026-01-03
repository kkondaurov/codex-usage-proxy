#![allow(dead_code)]

use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub display: DisplayConfig,
    #[serde(default)]
    pub sessions: SessionsConfig,
    #[serde(default)]
    pub pricing: PricingConfig,
    #[serde(default)]
    pub alerts: AlertConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            display: DisplayConfig::default(),
            sessions: SessionsConfig::default(),
            pricing: PricingConfig::default(),
            alerts: AlertConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut config = if let Some(path) = path {
            Self::from_file(path)?
        } else {
            let default_path = PathBuf::from("codex-usage.toml");
            if default_path.exists() {
                Self::from_file(&default_path)?
            } else {
                Self::default()
            }
        };

        config.apply_env_overrides();
        Ok(config)
    }

    fn from_file(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let config: Self =
            toml::from_str(&contents).with_context(|| "failed to parse configuration TOML")?;
        Ok(config)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(db_path) = env::var("CODEX_USAGE_DB_PATH") {
            self.storage.database_path = PathBuf::from(db_path);
        }
        if let Ok(sessions_dir) = env::var("CODEX_USAGE_SESSIONS_DIR") {
            self.sessions.root_dir = PathBuf::from(sessions_dir);
        }
        if let Ok(poll_interval) = env::var("CODEX_USAGE_SESSIONS_POLL_INTERVAL_SECS") {
            if let Ok(value) = poll_interval.parse::<u64>() {
                self.sessions.poll_interval_secs = value;
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_database_path")]
    pub database_path: PathBuf,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_path: default_database_path(),
            flush_interval_secs: default_flush_interval(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DisplayConfig {
    #[serde(default = "default_recent_capacity")]
    pub recent_events_capacity: usize,
    #[serde(default = "default_refresh_hz")]
    pub refresh_hz: u64,
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            recent_events_capacity: default_recent_capacity(),
            refresh_hz: default_refresh_hz(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionsConfig {
    #[serde(default = "default_sessions_root")]
    pub root_dir: PathBuf,
    #[serde(default = "default_sessions_poll_interval")]
    pub poll_interval_secs: u64,
}

impl Default for SessionsConfig {
    fn default() -> Self {
        Self {
            root_dir: default_sessions_root(),
            poll_interval_secs: default_sessions_poll_interval(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PricingConfig {
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub remote: RemotePricingConfig,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            currency: default_currency(),
            remote: RemotePricingConfig::default(),
        }
    }
}

impl PricingConfig {}

#[derive(Debug, Clone, Deserialize)]
pub struct RemotePricingConfig {
    #[serde(default = "default_pricing_url")]
    pub url: String,
    #[serde(default = "default_pricing_refresh_interval")]
    pub refresh_interval_hours: u64,
    #[serde(default = "default_pricing_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for RemotePricingConfig {
    fn default() -> Self {
        Self {
            url: default_pricing_url(),
            refresh_interval_hours: default_pricing_refresh_interval(),
            timeout_secs: default_pricing_timeout_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertConfig {
    #[serde(default)]
    pub daily_budget_usd: Option<f64>,
    #[serde(default)]
    pub monthly_budget_usd: Option<f64>,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            daily_budget_usd: None,
            monthly_budget_usd: None,
        }
    }
}

fn default_database_path() -> PathBuf {
    PathBuf::from("usage.db")
}

fn default_flush_interval() -> u64 {
    5
}

fn default_recent_capacity() -> usize {
    500
}

fn default_refresh_hz() -> u64 {
    10
}

fn default_sessions_root() -> PathBuf {
    if let Ok(home) = env::var("HOME") {
        PathBuf::from(home).join(".codex/sessions")
    } else {
        PathBuf::from(".codex/sessions")
    }
}

fn default_sessions_poll_interval() -> u64 {
    2
}

fn default_currency() -> String {
    "USD".to_string()
}

fn default_pricing_url() -> String {
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
        .to_string()
}

fn default_pricing_refresh_interval() -> u64 {
    24
}

fn default_pricing_timeout_secs() -> u64 {
    5
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env, fs,
        path::PathBuf,
        sync::{Mutex, OnceLock},
    };
    use tempfile::NamedTempFile;

    #[test]
    fn load_from_file_applies_overrides() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _db_guard = EnvGuard::unset("CODEX_USAGE_DB_PATH");

        let file = NamedTempFile::new().unwrap();
        let toml = r#"
            [storage]
            database_path = "custom.db"

            [display]
            recent_events_capacity = 77

            [pricing]
            currency = "EUR"

            [pricing.remote]
            url = "https://example.com/pricing.json"
            refresh_interval_hours = 12
            timeout_secs = 9
        "#;
        fs::write(file.path(), toml).unwrap();

        let config = AppConfig::load(Some(file.path())).unwrap();
        assert_eq!(config.storage.database_path, PathBuf::from("custom.db"));
        assert_eq!(config.display.recent_events_capacity, 77);
        assert_eq!(config.pricing.currency, "EUR");
        assert_eq!(
            config.pricing.remote.url,
            "https://example.com/pricing.json"
        );
        assert_eq!(config.pricing.remote.refresh_interval_hours, 12);
        assert_eq!(config.pricing.remote.timeout_secs, 9);
    }

    #[test]
    fn env_overrides_take_precedence() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _db_guard = EnvGuard::set("CODEX_USAGE_DB_PATH", "/tmp/codex-test.db");

        let config = AppConfig::load(None).unwrap();
        assert_eq!(
            config.storage.database_path,
            PathBuf::from("/tmp/codex-test.db")
        );
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = env::var(key).ok();
            unsafe { env::set_var(key, value) };
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = env::var(key).ok();
            if previous.is_some() {
                unsafe { env::remove_var(key) };
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(ref value) = self.previous {
                unsafe { env::set_var(self.key, value) };
            } else {
                unsafe { env::remove_var(self.key) };
            }
        }
    }

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
}
