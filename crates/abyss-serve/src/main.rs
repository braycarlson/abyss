mod config;

use std::net::SocketAddr;

use abyss_store::Store;
use anyhow::Context;
use rift::{RiotApi, RiotApiConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    // Serve connects without running migrations: schema changes are the
    // collector's job, so the public-facing binary needs no DDL power.
    let store = Store::connect(&config::database_url()).await?;

    let names = match abyss_static::Names::load().await {
        Ok(names) => {
            tracing::info!(patch = names.patch(), "loaded static data");

            Some(names)
        }
        Err(error) => {
            tracing::warn!(error = %error, "static data unavailable; serving ids without names");

            None
        }
    };

    let riot = build_api();

    if riot.is_some() {
        tracing::info!("riot id lookup enabled");
    } else {
        tracing::info!("no api key; riot id lookup disabled");
    }

    let addr: SocketAddr = config::address().parse().context("invalid ABYSS_ADDR")?;

    let api_config = config::api_config()?;
    let key_type = config::riot_key_type()?;

    if api_config.bearer_token.is_none() {
        tracing::warn!("ABYSS_API_TOKEN unset; read endpoints are public, mutating routes 403");
    }

    if key_type != "production" {
        tracing::warn!(
            key_type,
            "riot policy: public products require a production api key; \
             do not expose this service to the internet on this key"
        );
    }

    tracing::info!(
        auth = api_config.bearer_token.is_some(),
        cors_origin = api_config.cors_origin.as_str(),
        rate_limit_per_minute = api_config.rate_limit_per_minute,
        dragon_rate_limit_per_minute = api_config.dragon_rate_limit_per_minute,
        riot_rate_limit_per_minute = api_config.riot_rate_limit_per_minute,
        concurrent_requests_max = api_config.concurrent_requests_max,
        trusted_proxy = api_config.trusted_proxy,
        "api hardening"
    );

    abyss_api::serve(store, names, riot, addr, api_config, shutdown_signal()).await?;

    tracing::info!("serve stopped");

    Ok(())
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

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn build_api() -> Option<RiotApi> {
    let config = RiotApiConfig::from_env().ok()?;

    RiotApi::new(config).ok()
}
