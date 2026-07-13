mod config;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use abyss_collect::{Crawler, seed_ladders};
use abyss_core::{CrawlConfig, now_ms};
use abyss_store::Store;
use anyhow::{Context, anyhow};
use rift::{RiotApi, RiotApiConfig};
use tokio::sync::watch;

const REVISIT_TICK_MINUTES: u64 = 60;
const AGGREGATE_FORCE_MS: i64 = 6 * 3_600_000;
const AGGREGATE_PATCHES_MAX: usize = 1_024;

enum Command {
    Aggregate,
    Crawl,
    Migrate,
    Run,
    Seed,
    Stats,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let command = parse_command()?;

    match command {
        Command::Aggregate => cmd_aggregate().await,
        Command::Crawl => cmd_crawl().await,
        Command::Migrate => cmd_migrate().await,
        Command::Run => cmd_run().await,
        Command::Seed => cmd_seed().await,
        Command::Stats => cmd_stats().await,
    }
}

fn parse_command() -> anyhow::Result<Command> {
    let argument = std::env::args().nth(1);

    let command = match argument.as_deref() {
        Some("aggregate") => Command::Aggregate,
        Some("crawl") => Command::Crawl,
        Some("migrate") => Command::Migrate,
        Some("run") => Command::Run,
        Some("seed") => Command::Seed,
        Some("stats") => Command::Stats,
        other => {
            return Err(anyhow!(
                "usage: abyss <migrate|seed|crawl|aggregate [patch]|run|stats>, got {other:?}"
            ));
        }
    };

    Ok(command)
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn spawn_shutdown_listener() -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        shutdown_signal().await;

        let _ = shutdown_tx.send(true);
    });

    shutdown_rx
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let ctrl_c = tokio::signal::ctrl_c();

    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                _ = ctrl_c => {}
                _ = terminate.recv() => {}
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, "sigterm handler unavailable; ctrl-c only");

            let _ = ctrl_c.await;
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn build_api() -> anyhow::Result<RiotApi> {
    let config = RiotApiConfig::from_env().context("set RGAPI_KEY or RIOT_API_KEY")?;
    let api = RiotApi::new(config)?;

    Ok(api)
}

async fn cmd_migrate() -> anyhow::Result<()> {
    let store = Store::connect_and_migrate(&config::database_url()).await?;
    let counts = store.counts().await?;

    tracing::info!(?counts, "migrations applied");

    Ok(())
}

async fn cmd_seed() -> anyhow::Result<()> {
    let api = build_api()?;
    let store = Store::connect_and_migrate(&config::database_url()).await?;
    let crawl_config = config::crawl_config()?;

    let ladder = seed_ladders(&api, &store, &crawl_config).await?;

    let targets = config::seed_targets()?;
    let manual = abyss_collect::seed_targets(&api, &store, &targets).await?;

    tracing::info!(ladder, manual, "seed complete");

    Ok(())
}

async fn cmd_crawl() -> anyhow::Result<()> {
    let api = Arc::new(build_api()?);
    let store = Store::connect_and_migrate(&config::database_url()).await?;
    let crawl_config = Arc::new(config::crawl_config()?);

    let shutdown_rx = spawn_shutdown_listener();

    match abyss_static::current_patch().await {
        Ok(patch) => tracing::info!(patch = %patch, "current live patch"),
        Err(error) => tracing::warn!(error = %error, "could not fetch current patch"),
    }

    let crawler = Crawler::new(api, store, crawl_config);

    tracing::info!("crawl starting; press ctrl-c to stop");

    crawler.run(shutdown_rx).await?;

    Ok(())
}

async fn cmd_run() -> anyhow::Result<()> {
    let api = Arc::new(build_api()?);
    let store = Store::connect_and_migrate(&config::database_url()).await?;

    let mut crawl_config = config::crawl_config()?;
    crawl_config.service_mode = true;

    let crawl_config = Arc::new(crawl_config);
    let shutdown_rx = spawn_shutdown_listener();

    let seeded = seed_ladders(&api, &store, &crawl_config).await?;

    tracing::info!(seeded, "initial seed complete");

    let reseed_handle = tokio::spawn(reseed_loop(
        Arc::clone(&api),
        store.clone(),
        Arc::clone(&crawl_config),
        shutdown_rx.clone(),
    ));

    let aggregate_handle = tokio::spawn(aggregate_loop(
        store.clone(),
        Arc::clone(&crawl_config),
        shutdown_rx.clone(),
    ));

    let revisit_handle = tokio::spawn(revisit_loop(
        store.clone(),
        Arc::clone(&crawl_config),
        shutdown_rx.clone(),
    ));

    let crawler = Crawler::new(Arc::clone(&api), store, Arc::clone(&crawl_config));

    tracing::info!("service running; press ctrl-c to stop");

    crawler.run(shutdown_rx).await?;

    let _ = tokio::join!(reseed_handle, aggregate_handle, revisit_handle);

    tracing::info!("service stopped");

    Ok(())
}

async fn reseed_loop(
    api: Arc<RiotApi>,
    store: Store,
    crawl_config: Arc<CrawlConfig>,
    mut shutdown: watch::Receiver<bool>,
) {
    let interval = Duration::from_secs(u64::from(crawl_config.reseed_interval_hours) * 3_600);

    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => return,
        }

        if *shutdown.borrow() {
            return;
        }

        match seed_ladders(&api, &store, &crawl_config).await {
            Ok(seeded) => tracing::info!(seeded, "reseed complete"),
            Err(error) => tracing::error!(error = %error, "reseed failed"),
        }
    }
}

async fn aggregate_loop(
    store: Store,
    crawl_config: Arc<CrawlConfig>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut since = now_ms();

    if let Err(error) = store.rebuild_stats(now_ms(), None).await {
        tracing::error!(error = %error, "initial aggregate failed");
    }

    let startup_ms = now_ms();
    let interval = Duration::from_secs(u64::from(crawl_config.aggregate_interval_minutes) * 60);

    // A patch rebuild is a full recompute, so debounce: rebuild only once
    // enough new matches accumulate, with a force window so trickle patches
    // still converge.
    let mut pending: HashMap<String, i64> = HashMap::new();
    let mut rebuilt_at: HashMap<String, i64> = HashMap::new();

    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => return,
        }

        if *shutdown.borrow() {
            return;
        }

        let tick_start = now_ms();

        let fetched = match store.patches_fetched_since(since).await {
            Ok(counts) => counts,
            Err(error) => {
                tracing::error!(error = %error, "aggregate probe failed");

                continue;
            }
        };

        for (patch, count) in fetched {
            *pending.entry(patch).or_insert(0) += count;
        }

        assert!(
            pending.len() <= AGGREGATE_PATCHES_MAX,
            "pending patch set must stay bounded"
        );

        let now = now_ms();

        let due: Vec<String> = pending
            .iter()
            .filter(|&(patch, &count)| {
                let last = rebuilt_at
                    .get(patch.as_str())
                    .copied()
                    .unwrap_or(startup_ms);

                aggregate_due(count, last, now, crawl_config.aggregate_matches_min)
            })
            .map(|(patch, _)| patch.clone())
            .collect();

        for patch in due {
            match store.rebuild_stats(now_ms(), Some(&patch)).await {
                Ok(()) => {
                    pending.remove(&patch);
                    rebuilt_at.insert(patch, now_ms());
                }
                Err(error) => tracing::error!(error = %error, patch, "aggregate failed"),
            }
        }

        since = tick_start;
    }
}

fn aggregate_due(pending_matches: i64, rebuilt_at_ms: i64, now_ms: i64, matches_min: u32) -> bool {
    assert!(matches_min >= 1, "aggregate threshold must be positive");

    if pending_matches <= 0 {
        return false;
    }

    if pending_matches >= i64::from(matches_min) {
        return true;
    }

    now_ms.saturating_sub(rebuilt_at_ms) >= AGGREGATE_FORCE_MS
}

async fn revisit_loop(
    store: Store,
    crawl_config: Arc<CrawlConfig>,
    mut shutdown: watch::Receiver<bool>,
) {
    let interval = Duration::from_secs(REVISIT_TICK_MINUTES * 60);

    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => return,
        }

        if *shutdown.borrow() {
            return;
        }

        let ttl_ms = i64::from(crawl_config.revisit_after_hours) * 3_600_000;
        let cutoff = now_ms() - ttl_ms;

        match store
            .revisit_accounts(cutoff, i64::from(crawl_config.revisit_batch))
            .await
        {
            Ok(0) => {}
            Ok(requeued) => tracing::info!(requeued, "revisit requeued stale accounts"),
            Err(error) => tracing::error!(error = %error, "revisit failed"),
        }
    }
}

async fn cmd_stats() -> anyhow::Result<()> {
    let store = Store::connect_and_migrate(&config::database_url()).await?;

    let counts = store.counts().await?;
    let patches = store.patches_crawled().await?;
    let catalog = store.queue_catalog().await?;

    tracing::info!(?counts, "crawl counts");
    tracing::info!(patches = ?patches, "stored patches");

    for entry in &catalog {
        tracing::info!(
            queue_id = entry.queue_id,
            game_mode = entry.game_mode.as_str(),
            mutators = entry.mutators.as_str(),
            games = entry.games,
            "queue seen"
        );
    }

    Ok(())
}

async fn cmd_aggregate() -> anyhow::Result<()> {
    let store = Store::connect_and_migrate(&config::database_url()).await?;
    let patch = std::env::args().nth(2);

    abyss_aggregate::rebuild(&store, patch.as_deref()).await?;

    let counts = store.counts().await?;

    tracing::info!(
        ?counts,
        patch = patch.as_deref().unwrap_or("all"),
        "aggregate complete"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AGGREGATE_FORCE_MS, aggregate_due};

    #[test]
    fn aggregate_waits_below_threshold() {
        assert!(!aggregate_due(0, 0, 1_000, 50));
        assert!(!aggregate_due(49, 1_000, 2_000, 50));

        assert!(aggregate_due(50, 1_000, 2_000, 50));
        assert!(aggregate_due(5_000, 1_000, 2_000, 50));
    }

    #[test]
    fn aggregate_forces_stale_trickle_patches() {
        let rebuilt = 1_000;

        assert!(!aggregate_due(
            1,
            rebuilt,
            rebuilt + AGGREGATE_FORCE_MS - 1,
            50
        ));
        assert!(aggregate_due(1, rebuilt, rebuilt + AGGREGATE_FORCE_MS, 50));
    }
}
