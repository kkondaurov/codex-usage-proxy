use crate::{config::AppConfig, ingest, pricing_remote, storage::Storage, tui};
use anyhow::Result;
use std::sync::Arc;

/// High-level application orchestrator.
pub struct App {
    config: Arc<AppConfig>,
}

impl App {
    pub async fn new(config: AppConfig) -> Result<Self> {
        Ok(Self {
            config: Arc::new(config),
        })
    }

    pub async fn run(self, rebuild: bool) -> Result<()> {
        let storage = Storage::connect(&self.config.storage.database_path).await?;
        storage.ensure_schema().await?;
        if rebuild {
            tracing::info!("Rebuild requested: truncating usage tables");
            storage.truncate_usage_tables().await?;
        }

        let pricing_config = self.config.pricing.clone();
        let pricing_storage = storage.clone();
        tokio::spawn(async move {
            if let Err(err) =
                pricing_remote::sync_if_needed(&pricing_config, &pricing_storage).await
            {
                tracing::warn!(error = %err, "failed to sync pricing on startup");
            }
        });

        let ingest_handle = ingest::spawn(self.config.clone(), storage.clone()).await?;

        tracing::info!("Launching interactive TUI (requires an attached terminal)");
        tui::run(self.config.clone(), storage.clone()).await?;

        ingest_handle.shutdown().await?;
        Ok(())
    }
}
