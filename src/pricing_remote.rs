use crate::{
    config::PricingConfig,
    storage::{NewPrice, Storage},
};
use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate, Utc};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration as StdDuration;
use tokio::task;

const MILLION: f64 = 1_000_000.0;

#[derive(Debug, Deserialize, Clone)]
struct RemotePricing {
    input_cost_per_token: Option<f64>,
    output_cost_per_token: Option<f64>,
    cache_read_input_token_cost: Option<f64>,
}

pub async fn sync_if_needed(config: &PricingConfig, storage: &Storage) -> Result<bool> {
    let now = Utc::now();
    let refresh = Duration::hours(config.remote.refresh_interval_hours as i64);

    let existing = storage.prices_count().await?;
    let meta = storage.pricing_meta().await?;
    let due = match meta {
        Some(meta) => now.signed_duration_since(meta.last_fetch_at) >= refresh,
        None => true,
    };

    if !due && existing > 0 {
        return Ok(false);
    }

    match fetch_prices(config).await {
        Ok(prices) => {
            let count = storage
                .replace_prices(&prices, &config.remote.url, now)
                .await?;
            tracing::info!(count, "refreshed remote pricing");
            Ok(true)
        }
        Err(err) => {
            if existing > 0 {
                tracing::warn!(error = %err, "failed to refresh pricing; using cached prices");
                Ok(false)
            } else {
                Err(err)
            }
        }
    }
}

pub async fn force_sync(config: &PricingConfig, storage: &Storage) -> Result<usize> {
    let now = Utc::now();
    let prices = fetch_prices(config).await?;
    storage
        .replace_prices(&prices, &config.remote.url, now)
        .await
}

async fn fetch_prices(config: &PricingConfig) -> Result<Vec<NewPrice>> {
    let client = Client::builder()
        .timeout(StdDuration::from_secs(config.remote.timeout_secs.max(1)))
        .build()
        .with_context(|| "failed to build pricing http client")?;

    let response = client
        .get(&config.remote.url)
        .send()
        .await
        .with_context(|| "failed to fetch pricing dataset")?;

    let status = response.status();
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "pricing dataset request failed with {status}"
        ));
    }

    let bytes = response
        .bytes()
        .await
        .with_context(|| "failed to read pricing dataset")?;
    let currency = config.currency.clone();

    let prices = task::spawn_blocking(move || -> Result<Vec<NewPrice>> {
        let raw: HashMap<String, RemotePricing> =
            serde_json::from_slice(&bytes).with_context(|| "failed to parse pricing dataset")?;
        Ok(build_prices(raw, &currency))
    })
    .await
    .with_context(|| "pricing parse task failed")??;

    Ok(prices)
}

fn build_prices(raw: HashMap<String, RemotePricing>, currency: &str) -> Vec<NewPrice> {
    let mut resolved: HashMap<String, RemotePricing> = HashMap::new();

    for (key, record) in raw {
        let Some(model) = normalize_model_key(&key) else {
            continue;
        };
        resolved.entry(model).or_insert(record);
    }

    let effective_from = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let mut prices = Vec::with_capacity(resolved.len());
    for (model, record) in resolved {
        let Some(price) = normalize_pricing(&model, &record, currency, effective_from) else {
            continue;
        };
        prices.push(price);
    }

    prices.sort_by(|a, b| a.model.cmp(&b.model));
    prices
}

fn normalize_model_key(key: &str) -> Option<String> {
    let trimmed = key.trim();
    if let Some(rest) = trimmed.strip_prefix("openai/") {
        let value = rest.trim();
        return if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        };
    }
    if let Some(rest) = trimmed.strip_prefix("openai.") {
        let value = rest.trim();
        return if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        };
    }
    if looks_like_openai_model(trimmed) {
        return Some(trimmed.to_string());
    }
    None
}

fn looks_like_openai_model(value: &str) -> bool {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("gpt-") {
        return rest.chars().next().is_some_and(|ch| ch.is_ascii_digit());
    }
    if trimmed.starts_with('o') {
        return trimmed.chars().nth(1).is_some_and(|ch| ch.is_ascii_digit());
    }
    false
}

fn normalize_pricing(
    model: &str,
    record: &RemotePricing,
    currency: &str,
    effective_from: NaiveDate,
) -> Option<NewPrice> {
    let input = record.input_cost_per_token?;
    let output = record.output_cost_per_token?;

    let prompt_per_1m = input * MILLION;
    let completion_per_1m = output * MILLION;
    let cached_prompt_per_1m = record
        .cache_read_input_token_cost
        .map(|value| value * MILLION);

    Some(NewPrice {
        model: model.to_string(),
        effective_from,
        currency: currency.to_string(),
        prompt_per_1m,
        cached_prompt_per_1m,
        completion_per_1m,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_model_key_strips_openai_prefix() {
        assert_eq!(
            normalize_model_key("openai/gpt-test"),
            Some("gpt-test".to_string())
        );
        assert_eq!(
            normalize_model_key("openai.gpt-test"),
            Some("gpt-test".to_string())
        );
        assert_eq!(normalize_model_key("gpt-test"), None);
        assert_eq!(
            normalize_model_key("gpt-4o-mini"),
            Some("gpt-4o-mini".to_string())
        );
        assert_eq!(normalize_model_key("o4-mini"), Some("o4-mini".to_string()));
        assert_eq!(normalize_model_key("other/gpt-test"), None);
        assert_eq!(normalize_model_key("anthropic.claude-opus"), None);
    }

    #[test]
    fn build_prices_prefers_openai_entries() {
        let mut raw = HashMap::new();
        raw.insert(
            "gpt-test".to_string(),
            RemotePricing {
                input_cost_per_token: Some(1.0),
                output_cost_per_token: Some(2.0),
                cache_read_input_token_cost: None,
            },
        );
        raw.insert(
            "openai/gpt-test".to_string(),
            RemotePricing {
                input_cost_per_token: Some(3.0),
                output_cost_per_token: Some(4.0),
                cache_read_input_token_cost: Some(0.5),
            },
        );

        let prices = build_prices(raw, "USD");
        assert_eq!(prices.len(), 1);
        assert_eq!(prices[0].model, "gpt-test");
        assert!((prices[0].prompt_per_1m - 3_000_000.0).abs() < f64::EPSILON);
        assert!((prices[0].completion_per_1m - 4_000_000.0).abs() < f64::EPSILON);
        assert_eq!(prices[0].cached_prompt_per_1m, Some(500_000.0));
    }
}
