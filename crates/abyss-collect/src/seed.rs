use abyss_core::{CrawlConfig, Platform, SeedTarget, now_ms};
use abyss_store::Store;
use rift::RiotApi;
use rift::models::league_v4::LeagueListDto;

use crate::error::CollectError;

const QUEUE_RANKED_SOLO: &str = "RANKED_SOLO_5x5";
const SEED_PRIORITY_MANUAL: i32 = 0;

pub async fn seed_ladders(
    api: &RiotApi,
    store: &Store,
    config: &CrawlConfig,
) -> Result<u64, CollectError> {
    assert!(!config.regions.is_empty(), "no regions configured to seed");

    let mut seeded: u64 = 0;

    for &platform in &config.regions {
        let count = seed_platform(api, store, config, platform).await?;

        tracing::info!(platform = platform.as_str(), count, "seeded ladder");

        seeded += count;
    }

    Ok(seeded)
}

async fn seed_platform(
    api: &RiotApi,
    store: &Store,
    config: &CrawlConfig,
    platform: Platform,
) -> Result<u64, CollectError> {
    let route = platform.platform_route();
    let priority = config.priority_of(platform);
    let now = now_ms();

    let challenger = api
        .league_v4_get_challenger_league(route, QUEUE_RANKED_SOLO)
        .await?;

    let grandmaster = api
        .league_v4_get_grandmaster_league(route, QUEUE_RANKED_SOLO)
        .await?;

    let master = api
        .league_v4_get_master_league(route, QUEUE_RANKED_SOLO)
        .await?;

    let mut seeded: u64 = 0;

    seeded += enqueue_list(store, &challenger, platform, priority, now).await?;
    seeded += enqueue_list(store, &grandmaster, platform, priority, now).await?;
    seeded += enqueue_list(store, &master, platform, priority, now).await?;

    Ok(seeded)
}

async fn enqueue_list(
    store: &Store,
    list: &LeagueListDto,
    platform: Platform,
    priority: i32,
    now: i64,
) -> Result<u64, CollectError> {
    let puuids: Vec<String> = list
        .entries
        .iter()
        .map(|entry| entry.puuid.clone())
        .collect();

    let inserted = store
        .enqueue_puuids(&puuids, platform.as_str(), priority, now)
        .await?;

    Ok(inserted)
}

pub async fn seed_targets(
    api: &RiotApi,
    store: &Store,
    targets: &[SeedTarget],
) -> Result<u64, CollectError> {
    let now = now_ms();
    let mut seeded: u64 = 0;

    for target in targets {
        assert!(
            !target.game_name.is_empty(),
            "seed target game name must not be empty"
        );
        assert!(
            !target.tag_line.is_empty(),
            "seed target tag line must not be empty"
        );

        let route = target.platform.regional_group().regional_route();

        let account = api
            .account_v1_get_by_riot_id(route, &target.game_name, &target.tag_line)
            .await?;

        let Some(account) = account else {
            tracing::warn!(
                game_name = target.game_name,
                tag_line = target.tag_line,
                "seed account not found"
            );

            continue;
        };

        let inserted = store
            .enqueue_account(
                &account.puuid,
                target.platform.as_str(),
                SEED_PRIORITY_MANUAL,
                now,
            )
            .await?;

        if inserted {
            seeded += 1;
        }
    }

    Ok(seeded)
}
