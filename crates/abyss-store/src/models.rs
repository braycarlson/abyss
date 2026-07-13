use serde::Serialize;
use sqlx::FromRow;

#[derive(Clone, Debug)]
pub struct MatchRecord {
    pub match_id: String,
    pub platform: String,
    pub patch: String,
    pub queue_id: i32,
    pub game_mode: String,
    pub game_mutators: String,
    pub game_version: String,
    pub game_creation: i64,
    pub game_duration: i64,
    pub fetched_at: i64,
    pub raw_json: String,
}

#[derive(Clone, Debug)]
pub struct ParticipantRecord {
    pub match_id: String,
    pub puuid: String,
    pub patch: String,
    pub queue_id: i32,
    pub champion_id: i32,
    pub champion_name: String,
    pub team_id: i32,
    pub team_position: String,
    pub win: bool,
    pub kills: i32,
    pub deaths: i32,
    pub assists: i32,
    pub champion_level: i32,
    pub gold_earned: i32,
    pub damage_to_champions: i32,
    pub item0: i32,
    pub item1: i32,
    pub item2: i32,
    pub item3: i32,
    pub item4: i32,
    pub item5: i32,
    pub item6: i32,
    pub summoner1_id: i32,
    pub summoner2_id: i32,
    pub perk_keystone: i32,
    pub perk_primary_style: i32,
    pub perk_sub_style: i32,
    pub perk_primary_csv: String,
    pub perk_sub_csv: String,
    pub perk_shard_csv: String,
}

#[derive(Clone, Debug, FromRow)]
pub struct AccountClaim {
    pub puuid: String,
    pub platform: String,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct ChampionStat {
    pub patch: String,
    pub queue_id: i32,
    pub champion_id: i32,
    pub champion_name: String,
    pub games: i64,
    pub wins: i64,
    pub kills_sum: i64,
    pub deaths_sum: i64,
    pub assists_sum: i64,
    pub damage_sum: i64,
    pub gold_sum: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct MatchMeta {
    pub match_id: String,
    pub patch: String,
    pub queue_id: i32,
    pub game_mode: String,
    pub game_creation: i64,
    pub game_duration: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct MatchParticipant {
    pub puuid: String,
    pub champion_id: i32,
    pub champion_name: String,
    pub team_id: i32,
    pub win: bool,
    pub kills: i32,
    pub deaths: i32,
    pub assists: i32,
    pub champion_level: i32,
    pub gold_earned: i32,
    pub damage_to_champions: i32,
    pub item0: i32,
    pub item1: i32,
    pub item2: i32,
    pub item3: i32,
    pub item4: i32,
    pub item5: i32,
    pub item6: i32,
    pub summoner1_id: i32,
    pub summoner2_id: i32,
    pub perk_keystone: i32,
    pub perk_primary_style: i32,
    pub perk_sub_style: i32,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct QueueCatalogEntry {
    pub queue_id: i32,
    pub game_mode: String,
    pub mutators: String,
    pub games: i64,
    pub first_seen: i64,
    pub last_seen: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct CrawlStats {
    pub accounts_pending: i64,
    pub accounts_done: i64,
    pub matches_stored: i64,
    pub participants_stored: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct IdCount {
    pub id: i32,
    pub games: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct IdWinCount {
    pub id: i32,
    pub games: i64,
    pub wins: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct SpellPairCount {
    pub spell_a: i32,
    pub spell_b: i32,
    pub games: i64,
    pub wins: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct StylePairCount {
    pub primary_style: i32,
    pub sub_style: i32,
    pub games: i64,
    pub wins: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct SkillOrderCount {
    pub skill_max_order: String,
    pub games: i64,
    pub wins: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct RunePageCount {
    pub perk_primary_csv: String,
    pub perk_sub_csv: String,
    pub perk_shard_csv: String,
    pub perk_primary_style: i32,
    pub perk_sub_style: i32,
    pub games: i64,
    pub wins: i64,
}

#[derive(Clone, Debug, FromRow)]
pub struct MatchPending {
    pub match_id: String,
    pub platform: String,
}

#[derive(Clone, Debug)]
pub struct ParticipantTimelineRecord {
    pub match_id: String,
    pub puuid: String,
    pub patch: String,
    pub queue_id: i32,
    pub champion_id: i32,
    pub win: bool,
    pub skill_order: String,
    pub skill_max_order: String,
    pub skill_sequence: String,
    pub item_order: String,
    pub item_start: String,
    pub item_core: String,
    pub boots: i32,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct LabeledCount {
    pub label: String,
    pub games: i64,
    pub wins: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct PlayerChampionStat {
    pub champion_id: i32,
    pub champion_name: String,
    pub games: i64,
    pub wins: i64,
    pub kills_sum: i64,
    pub deaths_sum: i64,
    pub assists_sum: i64,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct PlayerMatch {
    pub match_id: String,
    pub patch: String,
    pub queue_id: i32,
    pub champion_id: i32,
    pub champion_name: String,
    pub win: bool,
    pub kills: i32,
    pub deaths: i32,
    pub assists: i32,
    pub game_creation: i64,
}
