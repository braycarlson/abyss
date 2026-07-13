use std::sync::Arc;

use abyss_core::{CrawlConfig, Patch, Platform, RegionalGroup, now_ms};
use abyss_static::Names;
use abyss_store::{
    MatchRecord, PARTICIPANTS_PER_MATCH_MAX, ParticipantRecord, ParticipantTimelineRecord, Store,
};
use rift::RiotApi;
use rift::endpoints::MatchV5GetMatchIdsByPuuidQuery;
use rift::models::match_v5::{MatchDto, ParticipantDto, PerksDto, TimelineDto};
use rift::routes::RegionalRoute;
use tokio::sync::watch;

use crate::error::CollectError;
use crate::timeline::{extract_builds, skill_priority};

const CLAIM_BATCH: i64 = 8;
const IDLE_SLEEP_MS: u64 = 2_000;
const IDLE_ROUNDS_MAX: u32 = 12;

const RECOVER_GRACE_MS: i64 = 60_000;
const RECOVER_MATCHES_MAX: i64 = 10_000;

const MATCH_FETCH_ATTEMPTS_MAX: i32 = 5;

const WORKER_FAILURES_MAX: u32 = 100;
const WORKER_BACKOFF_MS_MAX: u64 = 60_000;
const WORKER_BACKOFF_SHIFT_MAX: u32 = 6;

const PUUID_BRIEF_CHARS: usize = 8;

const SKILL_SEQUENCE_LENGTH_MAX: usize = 6;
const ITEM_START_LENGTH_MAX: usize = 3;
const ITEM_CORE_LENGTH_MAX: usize = 3;

pub struct Crawler {
    api: Arc<RiotApi>,
    store: Store,
    config: Arc<CrawlConfig>,
    names: Option<Arc<Names>>,
}

impl Crawler {
    #[must_use]
    pub fn new(api: Arc<RiotApi>, store: Store, config: Arc<CrawlConfig>) -> Crawler {
        assert!(!config.regions.is_empty(), "no regions configured to crawl");
        assert!(
            config.workers_per_route >= 1,
            "workers_per_route must be positive"
        );

        Crawler {
            api,
            store,
            config,
            names: None,
        }
    }

    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<(), CollectError> {
        let reset = self.store.reset_claimed().await?;

        tracing::info!(reset, "reset stuck claims");

        let names = load_classifier(&self.config).await;
        let groups = active_groups(&self.config.regions);

        let mut owned = self;
        owned.names = names;

        let crawler = Arc::new(owned);

        crawler.recover_pending_matches(&shutdown).await?;

        let maintenance = if crawler.config.service_mode {
            let crawler = Arc::clone(&crawler);
            let shutdown = shutdown.clone();

            Some(tokio::spawn(async move {
                crawler.maintenance_loop(shutdown).await;
            }))
        } else {
            None
        };

        let mut handles = Vec::new();

        for group in groups {
            let platforms = platforms_in_group(&crawler.config.regions, group);

            for _worker in 0..crawler.config.workers_per_route {
                let crawler = Arc::clone(&crawler);
                let shutdown = shutdown.clone();
                let platforms = platforms.clone();

                let handle =
                    tokio::spawn(async move { crawler.worker(group, &platforms, shutdown).await });

                handles.push(handle);
            }
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::error!(error = %error, "worker failed"),
                Err(error) => tracing::error!(error = %error, "worker panicked"),
            }
        }

        if let Some(handle) = maintenance {
            handle.abort();

            let _ = handle.await;
        }

        Ok(())
    }

    async fn maintenance_loop(&self, mut shutdown: watch::Receiver<bool>) {
        let interval =
            std::time::Duration::from_secs(u64::from(self.config.recover_interval_minutes) * 60);

        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                _ = shutdown.changed() => return,
            }

            if *shutdown.borrow() {
                return;
            }

            let claim_ttl_ms = i64::from(self.config.claim_ttl_minutes) * 60_000;
            let claim_cutoff = now_ms() - claim_ttl_ms;

            match self.store.reset_claimed_stale(claim_cutoff).await {
                Ok(0) => {}
                Ok(reset) => tracing::info!(reset, "reaped stale account claims"),
                Err(error) => tracing::error!(error = %error, "stale claim reap failed"),
            }

            if let Err(error) = self.recover_pending_matches(&shutdown).await {
                tracing::error!(error = %error, "match recovery failed");
            }
        }
    }

    async fn recover_pending_matches(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<(), CollectError> {
        let cutoff = now_ms() - RECOVER_GRACE_MS;

        let stale = self
            .store
            .stale_pending_matches(cutoff, RECOVER_MATCHES_MAX)
            .await?;

        if stale.is_empty() {
            return Ok(());
        }

        tracing::info!(count = stale.len(), "recovering unfetched matches");

        for pending in stale {
            if *shutdown.borrow() {
                return Ok(());
            }

            let platform = Platform::parse(&pending.platform)?;
            let route = platform.regional_group().regional_route();

            if let Err(error) = self.fetch_match(route, platform, &pending.match_id).await {
                self.fetch_match_failed(&pending.match_id, error).await?;
            }
        }

        Ok(())
    }

    async fn fetch_match_failed(
        &self,
        match_id: &str,
        error: CollectError,
    ) -> Result<(), CollectError> {
        match error {
            // A store failure says nothing about the match itself; propagate
            // so the row stays 'pending' for the recovery sweep.
            CollectError::Store(_) => Err(error),
            CollectError::Riot(ref riot) if riot.is_retriable() => {
                let skipped = self
                    .store
                    .mark_match_retry(match_id, MATCH_FETCH_ATTEMPTS_MAX)
                    .await?;

                if skipped {
                    tracing::warn!(error = %error, match_id, "match fetch retries exhausted");
                } else {
                    tracing::warn!(error = %error, match_id, "match fetch failed; queued for retry");
                }

                Ok(())
            }
            _ => {
                tracing::warn!(error = %error, match_id, "match fetch failed; skipped");

                self.store.mark_match_skipped(match_id).await?;

                Ok(())
            }
        }
    }

    async fn worker(
        &self,
        group: RegionalGroup,
        platforms: &[Platform],
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), CollectError> {
        assert!(!platforms.is_empty(), "worker requires platforms");

        let names = platforms
            .iter()
            .map(|platform| platform.as_str().to_string())
            .collect::<Vec<String>>();

        let mut idle: u32 = 0;
        let mut failures: u32 = 0;

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            match self.worker_tick(group, &names, &shutdown).await {
                Ok(true) => {
                    idle = 0;
                    failures = 0;
                }
                Ok(false) => {
                    idle = idle.saturating_add(1);
                    failures = 0;

                    if !self.config.service_mode && idle >= IDLE_ROUNDS_MAX {
                        return Ok(());
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(IDLE_SLEEP_MS)).await;
                }
                Err(error) => {
                    failures = failures.saturating_add(1);

                    // Consecutive failures mean the database or the network is
                    // gone; die loudly and let the supervisor restart us.
                    if failures >= WORKER_FAILURES_MAX {
                        return Err(error);
                    }

                    let backoff_ms = worker_backoff_ms(failures);

                    tracing::error!(error = %error, failures, backoff_ms, "worker tick failed");

                    tokio::select! {
                        () = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {}
                        _ = shutdown.changed() => return Ok(()),
                    }
                }
            }
        }
    }

    async fn worker_tick(
        &self,
        group: RegionalGroup,
        names: &[String],
        shutdown: &watch::Receiver<bool>,
    ) -> Result<bool, CollectError> {
        let claims = self
            .store
            .claim_accounts(names, CLAIM_BATCH, now_ms())
            .await?;

        if claims.is_empty() {
            return Ok(false);
        }

        for claim in claims {
            if *shutdown.borrow() {
                return Ok(true);
            }

            let platform = Platform::parse(&claim.platform)?;

            self.process_account(group, platform, &claim.puuid).await?;
        }

        Ok(true)
    }

    async fn process_account(
        &self,
        group: RegionalGroup,
        platform: Platform,
        puuid: &str,
    ) -> Result<(), CollectError> {
        assert!(!puuid.is_empty(), "puuid must not be empty");

        let route = group.regional_route();

        let match_ids = match self.collect_match_ids(route, puuid).await {
            Ok(ids) => ids,
            Err(error) => {
                tracing::warn!(error = %error, puuid = puuid_brief(puuid), "match id fetch failed");

                Vec::new()
            }
        };

        let new_ids = self
            .store
            .mark_matches_seen(&match_ids, platform.as_str(), now_ms())
            .await?;

        for match_id in new_ids {
            if let Err(error) = self.fetch_match(route, platform, &match_id).await {
                self.fetch_match_failed(&match_id, error).await?;
            }
        }

        self.store.mark_account_done(puuid, now_ms()).await?;

        Ok(())
    }

    async fn collect_match_ids(
        &self,
        route: RegionalRoute,
        puuid: &str,
    ) -> Result<Vec<String>, CollectError> {
        let ids_max = self.config.ids_per_account;

        assert!(ids_max > 0, "ids per account must be positive");

        if self.config.focus_queues.is_empty() {
            let query = MatchV5GetMatchIdsByPuuidQuery {
                start_time: self.config.since_unix,
                ..Default::default()
            };

            let ids = self
                .api
                .match_v5_match_ids_all(route, puuid, &query, ids_max)
                .await?;

            return Ok(ids);
        }

        let mut collected: Vec<String> = Vec::new();

        for &queue in &self.config.focus_queues {
            let query = MatchV5GetMatchIdsByPuuidQuery {
                queue: Some(i32::from(queue)),
                start_time: self.config.since_unix,
                ..Default::default()
            };

            let ids = self
                .api
                .match_v5_match_ids_all(route, puuid, &query, ids_max)
                .await?;

            collected.extend(ids);
        }

        Ok(collected)
    }

    async fn fetch_match(
        &self,
        route: RegionalRoute,
        platform: Platform,
        match_id: &str,
    ) -> Result<(), CollectError> {
        assert!(!match_id.is_empty(), "match id must not be empty");

        let Some((match_dto, raw_body)) = self.api.match_v5_get_match_raw(route, match_id).await?
        else {
            self.store.mark_match_skipped(match_id).await?;

            return Ok(());
        };

        let Some(patch) = Patch::from_game_version(&match_dto.info.game_version) else {
            self.store.mark_match_skipped(match_id).await?;

            return Ok(());
        };

        if let Some(floor) = self.config.patch_floor
            && patch < floor
        {
            self.store.mark_match_skipped(match_id).await?;

            return Ok(());
        }

        let raw_json = String::from_utf8(Vec::from(raw_body))?;
        let record = build_match_record(match_id, platform, patch, &match_dto, raw_json);
        let participants = build_participants(match_id, patch, &match_dto)?;

        self.store.store_match(&record, &participants).await?;

        if self.config.fetch_timeline {
            self.fetch_timeline(route, patch, match_id, &match_dto)
                .await?;
        }

        let priority = self.config.priority_of(platform);

        self.store
            .enqueue_puuids(
                &match_dto.metadata.participants,
                platform.as_str(),
                priority,
                now_ms(),
            )
            .await?;

        Ok(())
    }

    async fn fetch_timeline(
        &self,
        route: RegionalRoute,
        patch: Patch,
        match_id: &str,
        match_dto: &MatchDto,
    ) -> Result<(), CollectError> {
        let Some(timeline) = self.api.match_v5_get_timeline(route, match_id).await? else {
            return Ok(());
        };

        let records =
            build_timeline_records(match_id, patch, match_dto, &timeline, self.names.as_deref());

        if records.is_empty() {
            return Ok(());
        }

        self.store.store_timeline(&records).await?;

        Ok(())
    }
}

fn active_groups(regions: &[Platform]) -> Vec<RegionalGroup> {
    let mut groups: Vec<RegionalGroup> = Vec::new();

    for &platform in regions {
        let group = platform.regional_group();

        if !groups.contains(&group) {
            groups.push(group);
        }
    }

    groups
}

fn platforms_in_group(regions: &[Platform], group: RegionalGroup) -> Vec<Platform> {
    regions
        .iter()
        .copied()
        .filter(|platform| platform.regional_group() == group)
        .collect()
}

fn build_match_record(
    match_id: &str,
    platform: Platform,
    patch: Patch,
    dto: &MatchDto,
    raw_json: String,
) -> MatchRecord {
    assert!(!match_id.is_empty(), "match id must not be empty");
    assert!(!raw_json.is_empty(), "raw json must not be empty");

    let mutators = dto
        .info
        .game_mode_mutators
        .as_ref()
        .map(|values| values.join(","))
        .unwrap_or_default();

    MatchRecord {
        match_id: match_id.to_string(),
        platform: platform.as_str().to_string(),
        patch: patch.label(),
        queue_id: dto.info.queue_id,
        game_mode: dto.info.game_mode.clone(),
        game_mutators: mutators,
        game_version: dto.info.game_version.clone(),
        game_creation: dto.info.game_creation,
        game_duration: dto.info.game_duration,
        fetched_at: now_ms(),
        raw_json,
    }
}

fn build_participants(
    match_id: &str,
    patch: Patch,
    dto: &MatchDto,
) -> Result<Vec<ParticipantRecord>, CollectError> {
    // Network-provided data violating the participant bounds is an operating
    // error, not a programmer error: reject the match instead of crashing.
    if let Some(reason) = participants_invalid_reason(dto.info.participants.len()) {
        return Err(match_invalid(match_id, reason));
    }

    for participant in &dto.info.participants {
        if participant.puuid.is_empty() {
            return Err(match_invalid(match_id, "participant puuid is empty"));
        }
    }

    let queue_id = dto.info.queue_id;
    let patch_label = patch.label();

    let participants: Vec<ParticipantRecord> = dto
        .info
        .participants
        .iter()
        .map(|participant| participant_record(match_id, &patch_label, queue_id, participant))
        .collect();

    assert!(
        !participants.is_empty(),
        "participant bounds already checked"
    );
    assert!(
        participants.len() <= PARTICIPANTS_PER_MATCH_MAX,
        "participant bounds already checked"
    );

    Ok(participants)
}

fn participants_invalid_reason(count: usize) -> Option<&'static str> {
    if count == 0 {
        return Some("match has no participants");
    }

    if count > PARTICIPANTS_PER_MATCH_MAX {
        return Some("too many participants");
    }

    None
}

#[cold]
#[inline(never)]
fn match_invalid(match_id: &str, reason: &'static str) -> CollectError {
    CollectError::MatchInvalid {
        match_id: match_id.to_string(),
        reason,
    }
}

fn worker_backoff_ms(failures: u32) -> u64 {
    assert!(failures >= 1, "backoff requires at least one failure");

    let shift = failures.saturating_sub(1).min(WORKER_BACKOFF_SHIFT_MAX);
    let backoff_ms = 1_000_u64 << shift;

    backoff_ms.min(WORKER_BACKOFF_MS_MAX)
}

fn puuid_brief(puuid: &str) -> &str {
    let end = puuid
        .char_indices()
        .nth(PUUID_BRIEF_CHARS)
        .map_or(puuid.len(), |(index, _)| index);

    &puuid[..end]
}

fn participant_record(
    match_id: &str,
    patch_label: &str,
    queue_id: i32,
    participant: &ParticipantDto,
) -> ParticipantRecord {
    assert!(!match_id.is_empty(), "match id must not be empty");
    assert!(
        !participant.puuid.is_empty(),
        "participant puuid must not be empty"
    );

    let (perk_keystone, perk_primary_style, perk_sub_style) = perk_ids(&participant.perks);
    let (perk_primary_csv, perk_sub_csv, perk_shard_csv) = perk_csvs(&participant.perks);

    ParticipantRecord {
        match_id: match_id.to_string(),
        puuid: participant.puuid.clone(),
        patch: patch_label.to_string(),
        queue_id,
        champion_id: participant.champion_id,
        champion_name: participant.champion_name.clone(),
        team_id: participant.team_id,
        team_position: participant.team_position.clone(),
        win: participant.win,
        kills: participant.kills,
        deaths: participant.deaths,
        assists: participant.assists,
        champion_level: participant.champ_level,
        gold_earned: participant.gold_earned,
        damage_to_champions: participant.total_damage_dealt_to_champions,
        item0: participant.item0,
        item1: participant.item1,
        item2: participant.item2,
        item3: participant.item3,
        item4: participant.item4,
        item5: participant.item5,
        item6: participant.item6,
        summoner1_id: participant.summoner1_id,
        summoner2_id: participant.summoner2_id,
        perk_keystone,
        perk_primary_style,
        perk_sub_style,
        perk_primary_csv,
        perk_sub_csv,
        perk_shard_csv,
    }
}

fn perk_ids(perks: &PerksDto) -> (i32, i32, i32) {
    let keystone = perks
        .styles
        .first()
        .and_then(|style| style.selections.first())
        .map_or(0, |selection| selection.perk);

    let primary_style = perks.styles.first().map_or(0, |style| style.style);
    let sub_style = perks.styles.get(1).map_or(0, |style| style.style);

    (keystone, primary_style, sub_style)
}

fn perk_csvs(perks: &PerksDto) -> (String, String, String) {
    let primary = perks
        .styles
        .first()
        .map_or_else(String::new, style_selection_csv);
    let sub = perks
        .styles
        .get(1)
        .map_or_else(String::new, style_selection_csv);

    let shards = format!(
        "{},{},{}",
        perks.stat_perks.offense, perks.stat_perks.flex, perks.stat_perks.defense,
    );

    (primary, sub, shards)
}

fn style_selection_csv(style: &rift::models::match_v5::PerkStyleDto) -> String {
    let perks: Vec<i32> = style
        .selections
        .iter()
        .map(|selection| selection.perk)
        .collect();

    join_ids(&perks)
}

async fn load_classifier(config: &CrawlConfig) -> Option<Arc<Names>> {
    if !config.fetch_timeline {
        return None;
    }

    match Names::load().await {
        Ok(names) => {
            tracing::info!(
                patch = names.patch(),
                "loaded static data for build classification"
            );

            Some(Arc::new(names))
        }
        Err(error) => {
            tracing::warn!(error = %error, "static data unavailable; builds stay unclassified");

            None
        }
    }
}

fn build_timeline_records(
    match_id: &str,
    patch: Patch,
    match_dto: &MatchDto,
    timeline: &TimelineDto,
    names: Option<&Names>,
) -> Vec<ParticipantTimelineRecord> {
    let builds = extract_builds(timeline);
    let queue_id = match_dto.info.queue_id;
    let patch_label = patch.label();

    match_dto
        .info
        .participants
        .iter()
        .filter_map(|participant| {
            let raw = builds.get(&participant.participant_id)?;

            Some(ParticipantTimelineRecord {
                match_id: match_id.to_string(),
                puuid: participant.puuid.clone(),
                patch: patch_label.clone(),
                queue_id,
                champion_id: participant.champion_id,
                win: participant.win,
                skill_order: join_ids(&raw.skills),
                skill_max_order: skill_priority(&raw.skills),
                skill_sequence: skill_sequence_key(&raw.skills),
                item_order: join_ids(&raw.items),
                item_start: item_start_key(&raw.items),
                item_core: item_core_key(&raw.items, names),
                boots: boots_of(&raw.items, names),
            })
        })
        .collect()
}

fn join_ids(ids: &[i32]) -> String {
    ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
}

fn skill_sequence_key(skills: &[i32]) -> String {
    let take = skills.len().min(SKILL_SEQUENCE_LENGTH_MAX);

    join_ids(&skills[..take])
}

fn item_start_key(items: &[i32]) -> String {
    let mut start: Vec<i32> = items.iter().take(ITEM_START_LENGTH_MAX).copied().collect();

    start.sort_unstable();

    join_ids(&start)
}

fn item_core_key(items: &[i32], names: Option<&Names>) -> String {
    let Some(names) = names else {
        return String::new();
    };

    let mut core: Vec<i32> = Vec::new();

    for &item in items {
        if core.len() >= ITEM_CORE_LENGTH_MAX {
            break;
        }

        if names.item_is_core(item) && !core.contains(&item) {
            core.push(item);
        }
    }

    join_ids(&core)
}

fn boots_of(items: &[i32], names: Option<&Names>) -> i32 {
    let Some(names) = names else {
        return 0;
    };

    for &item in items {
        if names.item_is_boots(item) {
            return item;
        }
    }

    0
}

#[cfg(test)]
mod tests {
    use super::{
        PARTICIPANTS_PER_MATCH_MAX, WORKER_BACKOFF_MS_MAX, participants_invalid_reason,
        puuid_brief, worker_backoff_ms,
    };

    #[test]
    fn participants_bounds_reject_empty_and_oversized() {
        assert!(participants_invalid_reason(0).is_some());
        assert!(participants_invalid_reason(PARTICIPANTS_PER_MATCH_MAX + 1).is_some());

        assert!(participants_invalid_reason(1).is_none());
        assert!(participants_invalid_reason(10).is_none());
        assert!(participants_invalid_reason(PARTICIPANTS_PER_MATCH_MAX).is_none());
    }

    #[test]
    fn worker_backoff_doubles_and_caps() {
        assert_eq!(worker_backoff_ms(1), 1_000);
        assert_eq!(worker_backoff_ms(2), 2_000);
        assert_eq!(worker_backoff_ms(7), WORKER_BACKOFF_MS_MAX);
        assert_eq!(worker_backoff_ms(100), WORKER_BACKOFF_MS_MAX);
    }

    #[test]
    fn puuid_brief_truncates_safely() {
        assert_eq!(puuid_brief("abcdefghij"), "abcdefgh");
        assert_eq!(puuid_brief("short"), "short");
        assert_eq!(puuid_brief(""), "");
    }
}
