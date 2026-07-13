mod dragon;
mod middleware;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use abyss_core::{Platform, QUEUE_ARAM, now_ms};
use abyss_static::Names;
use abyss_store::{
    ChampionStat, CrawlStats, IdWinCount, LabeledCount, MatchMeta, MatchParticipant,
    PlayerChampionStat, PlayerMatch, QueueCatalogEntry, RunePageCount, SkillOrderCount,
    SpellPairCount, Store, StoreError, StylePairCount,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rift::RiotApi;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

pub use middleware::ApiConfig;

use crate::middleware::RateLimiter;

const BUILD_LIMIT: i64 = 15;
const TIER_GAMES_MIN: i64 = 20;
const MATCHUP_GAMES_MIN: i64 = 10;
const MATCHUP_LIMIT_DEFAULT: i64 = 200;
const MATCHUP_LIMIT_MAX: i64 = 500;
const PLAYER_LIMIT_DEFAULT: i64 = 20;
const PLAYER_LIMIT_MAX: i64 = 100;
const REFRESH_PRIORITY: i32 = -1;
const DEFAULT_REGION: &str = "NA1";
const BODY_BYTES_MAX: usize = 16 * 1024;

const ABOUT_DISCLAIMER: &str = "abyss is not endorsed by Riot Games and does not reflect the \
    views or opinions of Riot Games or anyone officially involved in producing or managing Riot \
    Games properties. Riot Games and League of Legends are trademarks or registered trademarks \
    of Riot Games, Inc.";

#[derive(Clone)]
struct AppState {
    store: Store,
    names: Option<Arc<Names>>,
    riot: Option<Arc<RiotApi>>,
    config: Arc<ApiConfig>,
    rate_limiter: Arc<RateLimiter>,
    dragon_rate_limiter: Arc<RateLimiter>,
    riot_rate_limiter: Arc<RateLimiter>,
    requests: Arc<Semaphore>,
    dragon_fetches: Arc<Semaphore>,
    dragon_cache_bytes: Arc<AtomicU64>,
    http: reqwest::Client,
}

pub async fn serve(
    store: Store,
    names: Option<Names>,
    riot: Option<RiotApi>,
    addr: SocketAddr,
    config: ApiConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error> {
    let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit_per_minute));
    let dragon_rate_limiter = Arc::new(RateLimiter::new(config.dragon_rate_limit_per_minute));
    let riot_rate_limiter = Arc::new(RateLimiter::new(config.riot_rate_limit_per_minute));
    let requests = Arc::new(Semaphore::new(config.concurrent_requests_max));
    let dragon_fetches = Arc::new(Semaphore::new(dragon::DRAGON_FETCHES_MAX));

    let cache_bytes = dragon::cache_bytes_scan(&config.dragon_dir);
    let dragon_cache_bytes = Arc::new(AtomicU64::new(cache_bytes));

    let http = reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(std::io::Error::other)?;

    let state = AppState {
        store,
        names: names.map(Arc::new),
        riot: riot.map(Arc::new),
        config: Arc::new(config),
        rate_limiter,
        dragon_rate_limiter,
        riot_rate_limiter,
        requests,
        dragon_fetches,
        dragon_cache_bytes,
        http,
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!(address = %addr, cache_bytes, "serving");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/about", get(about))
        .route("/patches", get(patches))
        .route("/queues", get(queues))
        .route("/stats", get(service_stats))
        .route("/aram/champions", get(aram_champions))
        .route("/aram/tier", get(aram_tier))
        .route("/aram/champions/{champion_id}/build", get(champion_build))
        .route(
            "/aram/champions/{champion_id}/matchups",
            get(champion_matchups),
        )
        .route(
            "/aram/champions/{champion_id}/synergies",
            get(champion_synergies),
        )
        .route("/players/by-riot-id/{name}/{tag}", get(player_by_riot_id))
        .route(
            "/players/by-riot-id/{name}/{tag}/refresh",
            post(player_refresh),
        )
        .route("/players/{puuid}/champions", get(player_champions))
        .route("/players/{puuid}/matches", get(player_matches))
        .route("/matches/{match_id}", get(match_detail))
        .route("/dragon/ddragon/{*path}", get(dragon::ddragon))
        .route("/dragon/cdragon/{*path}", get(dragon::cdragon))
        .route(
            "/dragon/csquare/{champion_id}",
            get(dragon::champion_square),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            middleware::guard,
        ))
        .layer(axum::extract::DefaultBodyLimit::max(BODY_BYTES_MAX))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Debug, Serialize)]
struct About {
    name: &'static str,
    version: &'static str,
    disclaimer: &'static str,
}

async fn about() -> Json<About> {
    Json(About {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        disclaimer: ABOUT_DISCLAIMER,
    })
}

async fn patches(State(state): State<AppState>) -> Result<Json<Vec<String>>, ApiError> {
    let patches = state.store.patches().await?;

    Ok(Json(patches))
}

async fn queues(State(state): State<AppState>) -> Result<Json<Vec<QueueCatalogEntry>>, ApiError> {
    let entries = state.store.queue_catalog().await?;

    Ok(Json(entries))
}

#[derive(Debug, Deserialize)]
struct ChampionQuery {
    patch: Option<String>,
    queue: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct TierQuery {
    patch: Option<String>,
    queue: Option<i32>,
    games_min: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RiotIdQuery {
    region: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ChampionRow {
    champion_id: i32,
    champion_name: String,
    games: i64,
    wins: i64,
    win_rate: f64,
    kda: f64,
    pick_rate: f64,
    kills_average: f64,
    deaths_average: f64,
    assists_average: f64,
    damage_average: f64,
    gold_average: f64,
}

async fn aram_champions(
    State(state): State<AppState>,
    Query(query): Query<ChampionQuery>,
) -> Result<Json<Vec<ChampionRow>>, ApiError> {
    let queue = query.queue.unwrap_or(i32::from(QUEUE_ARAM));

    let Some(patch) = resolve_patch(&state.store, query.patch).await? else {
        return Ok(Json(Vec::new()));
    };

    let (champions, total) = tokio::join!(
        state.store.champion_stats(&patch, queue),
        state.store.queue_total(&patch, queue),
    );

    let champions = champions?;
    let total = total?.unwrap_or(0);

    let rows = champions.iter().map(|entry| to_row(entry, total)).collect();

    Ok(Json(rows))
}

async fn aram_tier(
    State(state): State<AppState>,
    Query(query): Query<TierQuery>,
) -> Result<Json<Vec<ChampionRow>>, ApiError> {
    let queue = query.queue.unwrap_or(i32::from(QUEUE_ARAM));
    let games_min = query.games_min.unwrap_or(TIER_GAMES_MIN).max(1);

    debug_assert!(games_min >= 1, "games_min clamped below one");

    let Some(patch) = resolve_patch(&state.store, query.patch).await? else {
        return Ok(Json(Vec::new()));
    };

    let (champions, total) = tokio::join!(
        state.store.champion_stats_ranked(&patch, queue, games_min),
        state.store.queue_total(&patch, queue),
    );

    let champions = champions?;
    let total = total?.unwrap_or(0);

    let rows = champions.iter().map(|entry| to_row(entry, total)).collect();

    Ok(Json(rows))
}

#[derive(Debug, Serialize)]
struct BuildEntry {
    id: i32,
    name: Option<String>,
    games: i64,
    wins: i64,
    win_rate: f64,
}

#[derive(Debug, Serialize)]
struct BuildItem {
    id: i32,
    name: Option<String>,
}

#[derive(Debug, Serialize)]
struct ItemSetEntry {
    items: Vec<BuildItem>,
    games: i64,
    wins: i64,
    win_rate: f64,
}

#[derive(Debug, Serialize)]
struct SpellPairEntry {
    spell_a: i32,
    spell_a_name: Option<String>,
    spell_b: i32,
    spell_b_name: Option<String>,
    games: i64,
    wins: i64,
    win_rate: f64,
}

#[derive(Debug, Serialize)]
struct RuneStyleEntry {
    primary_style: i32,
    primary_name: Option<String>,
    sub_style: i32,
    sub_name: Option<String>,
    games: i64,
    wins: i64,
    win_rate: f64,
}

#[derive(Debug, Serialize)]
struct SkillOrderEntry {
    skill_max_order: String,
    games: i64,
    wins: i64,
    win_rate: f64,
}

#[derive(Debug, Serialize)]
struct SkillSequenceEntry {
    sequence: String,
    games: i64,
    wins: i64,
    win_rate: f64,
}

#[derive(Debug, Serialize)]
struct RunePageEntry {
    primary_style: i32,
    sub_style: i32,
    primary: Vec<BuildItem>,
    sub: Vec<BuildItem>,
    shards: Vec<BuildItem>,
    games: i64,
    wins: i64,
    win_rate: f64,
}

#[derive(Debug, Serialize)]
struct ChampionBuild {
    champion_id: i32,
    champion_name: Option<String>,
    patch: String,
    queue_id: i32,
    starting_items: Vec<ItemSetEntry>,
    core_builds: Vec<ItemSetEntry>,
    boots: Vec<BuildEntry>,
    items: Vec<BuildEntry>,
    keystones: Vec<BuildEntry>,
    summoner_spells: Vec<SpellPairEntry>,
    rune_styles: Vec<RuneStyleEntry>,
    rune_pages: Vec<RunePageEntry>,
    skill_orders: Vec<SkillOrderEntry>,
    skill_sequences: Vec<SkillSequenceEntry>,
}

async fn champion_build(
    State(state): State<AppState>,
    Path(champion_id): Path<i32>,
    Query(query): Query<ChampionQuery>,
) -> Result<Json<ChampionBuild>, ApiError> {
    if champion_id <= 0 {
        return Err(ApiError::BadRequest(
            "champion id must be positive".to_string(),
        ));
    }

    let queue = query.queue.unwrap_or(i32::from(QUEUE_ARAM));
    let names = state.names.as_deref();

    let Some(patch) = resolve_patch(&state.store, query.patch).await? else {
        return Ok(Json(empty_build(champion_id, queue, names)));
    };

    // Sequential on purpose: a 10-way join per request starves the 16-connection
    // pool under concurrent load; each read is an indexed rollup lookup.
    let db = &state.store;

    let items = db
        .champion_items(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let keystones = db
        .champion_keystones(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let spells = db
        .champion_spell_pairs(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let runes = db
        .champion_rune_styles(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let pages = db
        .champion_rune_pages(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let skills = db
        .champion_skill_orders(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let sequences = db
        .champion_skill_sequences(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let starts = db
        .champion_item_starts(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let cores = db
        .champion_item_cores(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;
    let boots = db
        .champion_boots(&patch, queue, champion_id, BUILD_LIMIT)
        .await?;

    let build = ChampionBuild {
        champion_id,
        champion_name: champion_name(names, champion_id),
        patch,
        queue_id: queue,
        starting_items: map_item_sets(&starts, names),
        core_builds: map_item_sets(&cores, names),
        boots: map_builds(&boots, names, Names::item),
        items: map_builds(&items, names, Names::item),
        keystones: map_builds(&keystones, names, Names::rune),
        summoner_spells: map_spells(&spells, names),
        rune_styles: map_runes(&runes, names),
        rune_pages: map_rune_pages(&pages, names),
        skill_orders: map_skills(&skills),
        skill_sequences: map_skill_sequences(&sequences),
    };

    Ok(Json(build))
}

fn empty_build(champion_id: i32, queue: i32, names: Option<&Names>) -> ChampionBuild {
    ChampionBuild {
        champion_id,
        champion_name: champion_name(names, champion_id),
        patch: String::new(),
        queue_id: queue,
        starting_items: Vec::new(),
        core_builds: Vec::new(),
        boots: Vec::new(),
        items: Vec::new(),
        keystones: Vec::new(),
        summoner_spells: Vec::new(),
        rune_styles: Vec::new(),
        rune_pages: Vec::new(),
        skill_orders: Vec::new(),
        skill_sequences: Vec::new(),
    }
}

#[derive(Debug, Deserialize)]
struct PairQuery {
    patch: Option<String>,
    queue: Option<i32>,
    games_min: Option<i64>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct PairRow {
    champion_id: i32,
    champion_name: Option<String>,
    games: i64,
    wins: i64,
    win_rate: f64,
}

async fn champion_matchups(
    State(state): State<AppState>,
    Path(champion_id): Path<i32>,
    Query(query): Query<PairQuery>,
) -> Result<Json<Vec<PairRow>>, ApiError> {
    if champion_id <= 0 {
        return Err(ApiError::BadRequest(
            "champion id must be positive".to_string(),
        ));
    }

    let queue = query.queue.unwrap_or(i32::from(QUEUE_ARAM));
    let games_min = query.games_min.unwrap_or(MATCHUP_GAMES_MIN).max(1);
    let limit = query
        .limit
        .unwrap_or(MATCHUP_LIMIT_DEFAULT)
        .clamp(1, MATCHUP_LIMIT_MAX);

    let Some(patch) = resolve_patch(&state.store, query.patch).await? else {
        return Ok(Json(Vec::new()));
    };

    let rows = state
        .store
        .champion_matchups(&patch, queue, champion_id, games_min, limit)
        .await?;

    Ok(Json(map_pairs(&rows, state.names.as_deref())))
}

async fn champion_synergies(
    State(state): State<AppState>,
    Path(champion_id): Path<i32>,
    Query(query): Query<PairQuery>,
) -> Result<Json<Vec<PairRow>>, ApiError> {
    if champion_id <= 0 {
        return Err(ApiError::BadRequest(
            "champion id must be positive".to_string(),
        ));
    }

    let queue = query.queue.unwrap_or(i32::from(QUEUE_ARAM));
    let games_min = query.games_min.unwrap_or(MATCHUP_GAMES_MIN).max(1);
    let limit = query
        .limit
        .unwrap_or(MATCHUP_LIMIT_DEFAULT)
        .clamp(1, MATCHUP_LIMIT_MAX);

    let Some(patch) = resolve_patch(&state.store, query.patch).await? else {
        return Ok(Json(Vec::new()));
    };

    let rows = state
        .store
        .champion_synergies(&patch, queue, champion_id, games_min, limit)
        .await?;

    Ok(Json(map_pairs(&rows, state.names.as_deref())))
}

fn map_pairs(rows: &[IdWinCount], names: Option<&Names>) -> Vec<PairRow> {
    rows.iter()
        .map(|row| PairRow {
            champion_id: row.id,
            champion_name: champion_name(names, row.id),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

#[derive(Debug, Serialize)]
struct RefreshResponse {
    puuid: String,
    queued: bool,
}

async fn player_refresh(
    State(state): State<AppState>,
    Path((name, tag)): Path<(String, String)>,
    Query(query): Query<RiotIdQuery>,
) -> Result<Json<RefreshResponse>, ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "riot id name must not be empty".to_string(),
        ));
    }

    if tag.is_empty() {
        return Err(ApiError::BadRequest(
            "riot id tag must not be empty".to_string(),
        ));
    }

    let Some(riot) = state.riot.as_deref() else {
        return Err(ApiError::BadRequest(
            "player refresh unavailable; serve started without an api key".to_string(),
        ));
    };

    let region = query.region.as_deref().unwrap_or(DEFAULT_REGION);
    let platform =
        Platform::parse(region).map_err(|error| ApiError::BadRequest(error.to_string()))?;

    let route = platform.regional_group().regional_route();
    let account = riot.account_v1_get_by_riot_id(route, &name, &tag).await?;

    let Some(account) = account else {
        return Err(ApiError::BadRequest("riot id not found".to_string()));
    };

    let queued = state
        .store
        .requeue_account(
            &account.puuid,
            platform.as_str(),
            REFRESH_PRIORITY,
            now_ms(),
        )
        .await?;

    let response = RefreshResponse {
        puuid: account.puuid,
        queued,
    };

    Ok(Json(response))
}

#[derive(Debug, Serialize)]
struct ServiceStats {
    counts: CrawlStats,
    patches: Vec<String>,
}

async fn service_stats(State(state): State<AppState>) -> Result<Json<ServiceStats>, ApiError> {
    let (counts, patches) = tokio::join!(state.store.counts_estimated(), state.store.patches());

    let body = ServiceStats {
        counts: counts?,
        patches: patches?,
    };

    Ok(Json(body))
}

#[derive(Debug, Serialize)]
struct PlayerChampionRow {
    champion_id: i32,
    champion_name: String,
    games: i64,
    wins: i64,
    win_rate: f64,
    kda: f64,
}

#[derive(Debug, Serialize)]
struct PlayerProfile {
    puuid: String,
    champions: Vec<PlayerChampionRow>,
    matches: Vec<PlayerMatch>,
}

async fn player_champions(
    State(state): State<AppState>,
    Path(puuid): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<PlayerChampionRow>>, ApiError> {
    let limit = clamp_limit(query.limit);
    let champions = state.store.player_champion_stats(&puuid, limit).await?;

    let rows = champions.iter().map(player_row).collect();

    Ok(Json(rows))
}

async fn player_matches(
    State(state): State<AppState>,
    Path(puuid): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<PlayerMatch>>, ApiError> {
    let limit = clamp_limit(query.limit);
    let matches = state.store.player_matches(&puuid, limit).await?;

    Ok(Json(matches))
}

#[derive(Debug, Serialize)]
struct MatchDetail {
    #[serde(flatten)]
    meta: MatchMeta,
    participants: Vec<MatchParticipant>,
}

async fn match_detail(
    State(state): State<AppState>,
    Path(match_id): Path<String>,
) -> Result<Json<MatchDetail>, ApiError> {
    if match_id.is_empty() {
        return Err(ApiError::BadRequest(
            "match id must not be empty".to_string(),
        ));
    }

    let Some(meta) = state.store.match_meta(&match_id).await? else {
        return Err(ApiError::NotFound("match not stored".to_string()));
    };

    let participants = state.store.match_participants(&match_id).await?;

    Ok(Json(MatchDetail { meta, participants }))
}

async fn player_by_riot_id(
    State(state): State<AppState>,
    Path((name, tag)): Path<(String, String)>,
    Query(query): Query<RiotIdQuery>,
) -> Result<Json<PlayerProfile>, ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "riot id name must not be empty".to_string(),
        ));
    }

    if tag.is_empty() {
        return Err(ApiError::BadRequest(
            "riot id tag must not be empty".to_string(),
        ));
    }

    let Some(riot) = state.riot.as_deref() else {
        return Err(ApiError::BadRequest(
            "riot id lookup unavailable; serve started without an api key".to_string(),
        ));
    };

    let region = query.region.as_deref().unwrap_or(DEFAULT_REGION);
    let platform =
        Platform::parse(region).map_err(|error| ApiError::BadRequest(error.to_string()))?;

    let route = platform.regional_group().regional_route();
    let account = riot.account_v1_get_by_riot_id(route, &name, &tag).await?;

    let Some(account) = account else {
        return Err(ApiError::BadRequest("riot id not found".to_string()));
    };

    let limit = clamp_limit(query.limit);
    let champions = state
        .store
        .player_champion_stats(&account.puuid, limit)
        .await?;
    let matches = state.store.player_matches(&account.puuid, limit).await?;

    let profile = PlayerProfile {
        puuid: account.puuid,
        champions: champions.iter().map(player_row).collect(),
        matches,
    };

    Ok(Json(profile))
}

fn clamp_limit(limit: Option<i64>) -> i64 {
    limit
        .unwrap_or(PLAYER_LIMIT_DEFAULT)
        .clamp(1, PLAYER_LIMIT_MAX)
}

fn champion_name(names: Option<&Names>, champion_id: i32) -> Option<String> {
    names
        .and_then(|resolver| resolver.champion(champion_id))
        .map(str::to_string)
}

fn map_builds(
    rows: &[IdWinCount],
    names: Option<&Names>,
    lookup: fn(&Names, i32) -> Option<&str>,
) -> Vec<BuildEntry> {
    rows.iter()
        .map(|row| BuildEntry {
            id: row.id,
            name: names
                .and_then(|resolver| lookup(resolver, row.id))
                .map(str::to_string),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

fn map_item_sets(rows: &[LabeledCount], names: Option<&Names>) -> Vec<ItemSetEntry> {
    rows.iter()
        .map(|row| ItemSetEntry {
            items: parse_items(&row.label, names),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

fn parse_items(csv: &str, names: Option<&Names>) -> Vec<BuildItem> {
    csv.split(',')
        .filter_map(|token| token.parse::<i32>().ok())
        .map(|id| BuildItem {
            id,
            name: names
                .and_then(|resolver| resolver.item(id))
                .map(str::to_string),
        })
        .collect()
}

fn map_rune_pages(rows: &[RunePageCount], names: Option<&Names>) -> Vec<RunePageEntry> {
    rows.iter()
        .map(|row| RunePageEntry {
            primary_style: row.perk_primary_style,
            sub_style: row.perk_sub_style,
            primary: parse_runes(&row.perk_primary_csv, names),
            sub: parse_runes(&row.perk_sub_csv, names),
            shards: parse_runes(&row.perk_shard_csv, names),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

fn parse_runes(csv: &str, names: Option<&Names>) -> Vec<BuildItem> {
    csv.split(',')
        .filter_map(|token| token.parse::<i32>().ok())
        .map(|id| BuildItem {
            id,
            name: names
                .and_then(|resolver| resolver.rune(id))
                .map(str::to_string),
        })
        .collect()
}

fn map_skills(rows: &[SkillOrderCount]) -> Vec<SkillOrderEntry> {
    rows.iter()
        .map(|row| SkillOrderEntry {
            skill_max_order: row.skill_max_order.clone(),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

fn map_skill_sequences(rows: &[LabeledCount]) -> Vec<SkillSequenceEntry> {
    rows.iter()
        .map(|row| SkillSequenceEntry {
            sequence: slots_to_letters(&row.label),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

fn slots_to_letters(csv: &str) -> String {
    csv.split(',')
        .map(|slot| match slot {
            "1" => "Q",
            "2" => "W",
            "3" => "E",
            "4" => "R",
            _ => "?",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn map_spells(rows: &[SpellPairCount], names: Option<&Names>) -> Vec<SpellPairEntry> {
    rows.iter()
        .map(|row| SpellPairEntry {
            spell_a: row.spell_a,
            spell_a_name: names
                .and_then(|resolver| resolver.spell(row.spell_a))
                .map(str::to_string),
            spell_b: row.spell_b,
            spell_b_name: names
                .and_then(|resolver| resolver.spell(row.spell_b))
                .map(str::to_string),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

fn map_runes(rows: &[StylePairCount], names: Option<&Names>) -> Vec<RuneStyleEntry> {
    rows.iter()
        .map(|row| RuneStyleEntry {
            primary_style: row.primary_style,
            primary_name: names
                .and_then(|resolver| resolver.rune(row.primary_style))
                .map(str::to_string),
            sub_style: row.sub_style,
            sub_name: names
                .and_then(|resolver| resolver.rune(row.sub_style))
                .map(str::to_string),
            games: row.games,
            wins: row.wins,
            win_rate: win_rate(row.wins, row.games),
        })
        .collect()
}

#[allow(clippy::cast_precision_loss)]
fn player_row(stat: &PlayerChampionStat) -> PlayerChampionRow {
    debug_assert!(stat.games >= 0, "game count must not be negative");
    debug_assert!(stat.wins <= stat.games, "wins cannot exceed games");

    let deaths = stat.deaths_sum.max(1) as f64;

    PlayerChampionRow {
        champion_id: stat.champion_id,
        champion_name: stat.champion_name.clone(),
        games: stat.games,
        wins: stat.wins,
        win_rate: win_rate(stat.wins, stat.games),
        kda: (stat.kills_sum + stat.assists_sum) as f64 / deaths,
    }
}

#[allow(clippy::cast_precision_loss)]
fn win_rate(wins: i64, games: i64) -> f64 {
    debug_assert!(wins >= 0, "win count must not be negative");
    debug_assert!(games >= 0, "game count must not be negative");
    debug_assert!(wins <= games, "wins cannot exceed games");

    wins as f64 / games.max(1) as f64
}

async fn resolve_patch(
    store: &Store,
    requested: Option<String>,
) -> Result<Option<String>, StoreError> {
    if let Some(patch) = requested
        && !patch.trim().is_empty()
    {
        return Ok(Some(patch));
    }

    let patches = store.patches().await?;

    Ok(patches.into_iter().next())
}

#[allow(clippy::cast_precision_loss)]
fn to_row(stat: &ChampionStat, matches_total: i64) -> ChampionRow {
    debug_assert!(stat.games >= 0, "game count must not be negative");
    debug_assert!(stat.wins <= stat.games, "wins cannot exceed games");
    debug_assert!(matches_total >= 0, "match total must not be negative");

    let games = stat.games.max(1) as f64;
    let deaths = stat.deaths_sum.max(1) as f64;

    let pick_rate = if matches_total > 0 {
        stat.games as f64 / matches_total as f64
    } else {
        0.0
    };

    ChampionRow {
        champion_id: stat.champion_id,
        champion_name: stat.champion_name.clone(),
        games: stat.games,
        wins: stat.wins,
        win_rate: stat.wins as f64 / games,
        kda: (stat.kills_sum + stat.assists_sum) as f64 / deaths,
        pick_rate,
        kills_average: stat.kills_sum as f64 / games,
        deaths_average: stat.deaths_sum as f64 / games,
        assists_average: stat.assists_sum as f64 / games,
        damage_average: stat.damage_sum as f64 / games,
        gold_average: stat.gold_sum as f64 / games,
    }
}

enum ApiError {
    Store(StoreError),
    Riot(rift::Error),
    BadRequest(String),
    NotFound(String),
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> ApiError {
        ApiError::Store(error)
    }
}

impl From<rift::Error> for ApiError {
    fn from(error: rift::Error) -> ApiError {
        ApiError::Riot(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Internal error detail stays in the server log; clients get a generic
        // message so database and upstream internals never leak.
        let (status, message) = match self {
            ApiError::Store(error) => {
                tracing::error!(error = %error, "store request failed");

                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
            ApiError::Riot(error) => {
                tracing::error!(error = %error, "riot request failed");

                (StatusCode::BAD_GATEWAY, "upstream unavailable".to_string())
            }
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            ApiError::NotFound(message) => (StatusCode::NOT_FOUND, message),
        };

        (status, message).into_response()
    }
}
