use std::env;

use anyhow::{Context, anyhow};

const DATABASE_URL_DEFAULT: &str = "postgres://abyss:abyss@localhost:5432/abyss";
const ADDRESS_DEFAULT: &str = "0.0.0.0:8080";
const RATE_LIMIT_PER_MINUTE_DEFAULT: u32 = 120;
const RATE_LIMIT_PER_MINUTE_MAX: u32 = 100_000;
const DRAGON_RATE_LIMIT_PER_MINUTE_DEFAULT: u32 = 1_200;
const RIOT_RATE_LIMIT_PER_MINUTE_DEFAULT: u32 = 10;
const CONCURRENT_REQUESTS_MAX_DEFAULT: u32 = 256;
const CONCURRENT_REQUESTS_MAX_LIMIT: u32 = 4_096;
const DRAGON_CACHE_BYTES_MAX_DEFAULT: u64 = 1_073_741_824;
const DRAGON_CACHE_BYTES_MAX_MIN: u64 = 1_048_576;
const CORS_ORIGIN_DEFAULT: &str = "*";
const DRAGON_DIR_DEFAULT: &str = "dragon-cache";
const RIOT_KEY_TYPE_DEFAULT: &str = "development";

pub fn database_url() -> String {
    env::var("ABYSS_DATABASE_URL").unwrap_or_else(|_| DATABASE_URL_DEFAULT.to_string())
}

pub fn address() -> String {
    env::var("ABYSS_ADDR").unwrap_or_else(|_| ADDRESS_DEFAULT.to_string())
}

pub fn riot_key_type() -> anyhow::Result<String> {
    let value = env::var("ABYSS_RIOT_KEY_TYPE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| RIOT_KEY_TYPE_DEFAULT.to_string());

    let normalized = value.trim().to_ascii_lowercase();

    match normalized.as_str() {
        "development" | "personal" | "production" => Ok(normalized),
        other => Err(anyhow!(
            "invalid ABYSS_RIOT_KEY_TYPE {other:?}; expected development, personal, or production"
        )),
    }
}

pub fn api_config() -> anyhow::Result<abyss_api::ApiConfig> {
    let bearer_token = match env::var("ABYSS_API_TOKEN") {
        Ok(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
        _ => None,
    };

    let cors_origin = env::var("ABYSS_CORS_ORIGIN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| CORS_ORIGIN_DEFAULT.to_string());

    let rate_limit_per_minute =
        env_u32("ABYSS_RATE_LIMIT_PER_MINUTE", RATE_LIMIT_PER_MINUTE_DEFAULT)?
            .clamp(1, RATE_LIMIT_PER_MINUTE_MAX);

    let dragon_rate_limit_per_minute = env_u32(
        "ABYSS_DRAGON_RATE_LIMIT_PER_MINUTE",
        DRAGON_RATE_LIMIT_PER_MINUTE_DEFAULT,
    )?
    .clamp(1, RATE_LIMIT_PER_MINUTE_MAX);

    let riot_rate_limit_per_minute = env_u32(
        "ABYSS_RIOT_RATE_LIMIT_PER_MINUTE",
        RIOT_RATE_LIMIT_PER_MINUTE_DEFAULT,
    )?
    .clamp(1, RATE_LIMIT_PER_MINUTE_MAX);

    let concurrent_requests_max = env_u32(
        "ABYSS_CONCURRENT_REQUESTS_MAX",
        CONCURRENT_REQUESTS_MAX_DEFAULT,
    )?
    .clamp(1, CONCURRENT_REQUESTS_MAX_LIMIT);

    let dragon_cache_bytes_max = env_u64(
        "ABYSS_DRAGON_CACHE_BYTES_MAX",
        DRAGON_CACHE_BYTES_MAX_DEFAULT,
    )?
    .max(DRAGON_CACHE_BYTES_MAX_MIN);

    let trusted_proxy = env_bool("ABYSS_TRUSTED_PROXY", false)?;

    let dragon_dir = env::var("ABYSS_DRAGON_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DRAGON_DIR_DEFAULT.to_string())
        .into();

    let concurrent_requests_max =
        usize::try_from(concurrent_requests_max).context("concurrent requests max fits usize")?;

    Ok(abyss_api::ApiConfig {
        bearer_token,
        concurrent_requests_max,
        cors_origin,
        dragon_cache_bytes_max,
        dragon_dir,
        dragon_rate_limit_per_minute,
        rate_limit_per_minute,
        riot_rate_limit_per_minute,
        trusted_proxy,
    })
}

fn env_u32(key: &str, default: u32) -> anyhow::Result<u32> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => {
            let parsed = value
                .trim()
                .parse::<u32>()
                .with_context(|| format!("invalid {key}"))?;

            Ok(parsed)
        }
        _ => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => {
            let parsed = value
                .trim()
                .parse::<u64>()
                .with_context(|| format!("invalid {key}"))?;

            Ok(parsed)
        }
        _ => Ok(default),
    }
}

fn env_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            other => Err(anyhow!("invalid {key}: {other:?}; expected true or false")),
        },
        _ => Ok(default),
    }
}
