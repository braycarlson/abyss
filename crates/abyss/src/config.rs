use std::env;

use abyss_core::config::{
    AGGREGATE_INTERVAL_MINUTES_MAX, AGGREGATE_MATCHES_MIN_MAX, CLAIM_TTL_MINUTES_MAX,
    CLAIM_TTL_MINUTES_MIN, IDS_PER_ACCOUNT_MAX, RECOVER_INTERVAL_MINUTES_MAX,
    RESEED_INTERVAL_HOURS_MAX, REVISIT_AFTER_HOURS_MAX, REVISIT_BATCH_MAX, WORKERS_PER_ROUTE_MAX,
};
use abyss_core::{CrawlConfig, Patch, Platform, QUEUE_ARAM, SeedTarget};
use anyhow::{Context, anyhow};

const DATABASE_URL_DEFAULT: &str = "postgres://abyss:abyss@localhost:5432/abyss";
const WORKERS_DEFAULT: u32 = 4;
const IDS_PER_ACCOUNT_DEFAULT: u32 = 50;
const RESEED_INTERVAL_HOURS_DEFAULT: u32 = 24;
const AGGREGATE_INTERVAL_MINUTES_DEFAULT: u32 = 15;
const AGGREGATE_MATCHES_MIN_DEFAULT: u32 = 50;
const REVISIT_AFTER_HOURS_DEFAULT: u32 = 24;
const REVISIT_BATCH_DEFAULT: u32 = 10_000;
const RECOVER_INTERVAL_MINUTES_DEFAULT: u32 = 10;
const CLAIM_TTL_MINUTES_DEFAULT: u32 = 30;

pub fn database_url() -> String {
    env::var("ABYSS_DATABASE_URL").unwrap_or_else(|_| DATABASE_URL_DEFAULT.to_string())
}

pub fn crawl_config() -> anyhow::Result<CrawlConfig> {
    let regions = regions()?;
    let focus_queues = queues()?;
    let patch_floor = patch_floor()?;
    let since_unix = since_unix()?;

    let workers_per_route =
        env_u32("ABYSS_WORKERS", WORKERS_DEFAULT)?.clamp(1, WORKERS_PER_ROUTE_MAX);

    let ids_per_account =
        env_u32("ABYSS_IDS_PER_ACCOUNT", IDS_PER_ACCOUNT_DEFAULT)?.clamp(1, IDS_PER_ACCOUNT_MAX);

    let fetch_timeline = env_bool("ABYSS_TIMELINE", false)?;

    let reseed_interval_hours = env_u32("ABYSS_RESEED_HOURS", RESEED_INTERVAL_HOURS_DEFAULT)?
        .clamp(1, RESEED_INTERVAL_HOURS_MAX);

    let aggregate_interval_minutes = env_u32(
        "ABYSS_AGGREGATE_MINUTES",
        AGGREGATE_INTERVAL_MINUTES_DEFAULT,
    )?
    .clamp(1, AGGREGATE_INTERVAL_MINUTES_MAX);

    let aggregate_matches_min =
        env_u32("ABYSS_AGGREGATE_MATCHES_MIN", AGGREGATE_MATCHES_MIN_DEFAULT)?
            .clamp(1, AGGREGATE_MATCHES_MIN_MAX);

    let revisit_after_hours = env_u32("ABYSS_REVISIT_HOURS", REVISIT_AFTER_HOURS_DEFAULT)?
        .clamp(1, REVISIT_AFTER_HOURS_MAX);

    let revisit_batch =
        env_u32("ABYSS_REVISIT_BATCH", REVISIT_BATCH_DEFAULT)?.clamp(1, REVISIT_BATCH_MAX);

    let recover_interval_minutes =
        env_u32("ABYSS_RECOVER_MINUTES", RECOVER_INTERVAL_MINUTES_DEFAULT)?
            .clamp(1, RECOVER_INTERVAL_MINUTES_MAX);

    let claim_ttl_minutes = env_u32("ABYSS_CLAIM_TTL_MINUTES", CLAIM_TTL_MINUTES_DEFAULT)?
        .clamp(CLAIM_TTL_MINUTES_MIN, CLAIM_TTL_MINUTES_MAX);

    Ok(CrawlConfig {
        regions,
        focus_queues,
        patch_floor,
        since_unix,
        workers_per_route,
        ids_per_account,
        fetch_timeline,
        service_mode: false,
        reseed_interval_hours,
        aggregate_interval_minutes,
        aggregate_matches_min,
        revisit_after_hours,
        revisit_batch,
        recover_interval_minutes,
        claim_ttl_minutes,
    })
}

fn regions() -> anyhow::Result<Vec<Platform>> {
    let raw = match env::var("ABYSS_REGIONS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(default_regions()),
    };

    let mut platforms: Vec<Platform> = Vec::new();

    for token in raw.split(',') {
        let token = token.trim();

        if token.is_empty() {
            continue;
        }

        let platform =
            Platform::parse(token).with_context(|| format!("invalid region: {token}"))?;

        platforms.push(platform);
    }

    if platforms.is_empty() {
        return Ok(default_regions());
    }

    Ok(platforms)
}

fn default_regions() -> Vec<Platform> {
    vec![Platform::Kr, Platform::Euw1, Platform::Na1]
}

fn queues() -> anyhow::Result<Vec<u16>> {
    let Ok(raw) = env::var("ABYSS_QUEUES") else {
        return Ok(vec![QUEUE_ARAM]);
    };

    let trimmed = raw.trim();

    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("all") {
        return Ok(Vec::new());
    }

    let mut queues: Vec<u16> = Vec::new();

    for token in trimmed.split(',') {
        let token = token.trim();

        if token.is_empty() {
            continue;
        }

        let queue = token
            .parse::<u16>()
            .with_context(|| format!("invalid queue id: {token}"))?;

        queues.push(queue);
    }

    Ok(queues)
}

fn patch_floor() -> anyhow::Result<Option<Patch>> {
    match env::var("ABYSS_PATCH_FLOOR") {
        Ok(value) if !value.trim().is_empty() => Ok(Some(Patch::parse(value.trim())?)),
        _ => Ok(None),
    }
}

fn since_unix() -> anyhow::Result<Option<i64>> {
    match env::var("ABYSS_SINCE_UNIX") {
        Ok(value) if !value.trim().is_empty() => {
            let seconds = value
                .trim()
                .parse::<i64>()
                .context("invalid ABYSS_SINCE_UNIX")?;

            Ok(Some(seconds))
        }
        _ => Ok(None),
    }
}

pub fn seed_targets() -> anyhow::Result<Vec<SeedTarget>> {
    let Ok(raw) = env::var("ABYSS_SEED") else {
        return Ok(Vec::new());
    };

    let mut targets: Vec<SeedTarget> = Vec::new();

    for token in raw.split(',') {
        let token = token.trim();

        if token.is_empty() {
            continue;
        }

        targets.push(SeedTarget::parse(token)?);
    }

    Ok(targets)
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

fn env_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    let Ok(value) = env::var(key) else {
        return Ok(default);
    };

    let normalized = value.trim().to_ascii_lowercase();

    match normalized.as_str() {
        "" => Ok(default),
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(anyhow!("invalid {key}, expected a boolean")),
    }
}
