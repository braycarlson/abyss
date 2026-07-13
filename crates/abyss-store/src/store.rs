use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::error::StoreError;
use crate::models::{
    AccountClaim, ChampionStat, CrawlStats, IdWinCount, LabeledCount, MatchMeta, MatchParticipant,
    MatchPending, MatchRecord, ParticipantRecord, ParticipantTimelineRecord, PlayerChampionStat,
    PlayerMatch, QueueCatalogEntry, RunePageCount, SkillOrderCount, SpellPairCount, StylePairCount,
};

const CONNECTIONS_MAX: u32 = 16;
pub const PARTICIPANTS_PER_MATCH_MAX: usize = 16;

const BATCH_CHUNK_MAX: usize = 500;
const BATCH_TOTAL_MAX: usize = 100_000;

type Transaction = sqlx::Transaction<'static, sqlx::Postgres>;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Store, StoreError> {
        assert!(!url.is_empty(), "database url must not be empty");

        let pool = PgPoolOptions::new()
            .max_connections(CONNECTIONS_MAX)
            .connect(url)
            .await?;

        Ok(Store { pool })
    }

    pub async fn connect_and_migrate(url: &str) -> Result<Store, StoreError> {
        let store = Store::connect(url).await?;

        sqlx::migrate!("../../migrations").run(&store.pool).await?;

        Ok(store)
    }

    pub async fn reset_claimed(&self) -> Result<u64, StoreError> {
        let result = sqlx::query("UPDATE account SET status = 'pending' WHERE status = 'claimed'")
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }

    pub async fn reset_claimed_stale(&self, cutoff: i64) -> Result<u64, StoreError> {
        assert!(cutoff > 0, "cutoff must be positive");

        // crawled_at is stamped at claim time, so a claimed row older than the
        // cutoff belongs to a dead worker and goes back to the frontier.
        let result = sqlx::query(
            "UPDATE account SET status = 'pending' \
             WHERE status = 'claimed' AND crawled_at < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn enqueue_account(
        &self,
        puuid: &str,
        platform: &str,
        priority: i32,
        now: i64,
    ) -> Result<bool, StoreError> {
        assert!(!puuid.is_empty(), "puuid must not be empty");
        assert!(!platform.is_empty(), "platform must not be empty");

        let result = sqlx::query(
            "INSERT INTO account (puuid, platform, priority, status, discovered_at) \
             VALUES ($1, $2, $3, 'pending', $4) \
             ON CONFLICT DO NOTHING",
        )
        .bind(puuid)
        .bind(platform)
        .bind(priority)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn enqueue_puuids(
        &self,
        puuids: &[String],
        platform: &str,
        priority: i32,
        now: i64,
    ) -> Result<u64, StoreError> {
        assert!(!platform.is_empty(), "platform must not be empty");
        assert!(
            puuids.len() <= BATCH_TOTAL_MAX,
            "too many puuids in one enqueue"
        );

        let mut inserted: u64 = 0;

        for chunk in puuids.chunks(BATCH_CHUNK_MAX) {
            let result = sqlx::query(
                "INSERT INTO account (puuid, platform, priority, status, discovered_at) \
                 SELECT puuid, $2, $3, 'pending', $4 FROM unnest($1::text[]) AS t(puuid) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(chunk)
            .bind(platform)
            .bind(priority)
            .bind(now)
            .execute(&self.pool)
            .await?;

            inserted += result.rows_affected();
        }

        Ok(inserted)
    }

    pub async fn claim_accounts(
        &self,
        platforms: &[String],
        limit: i64,
        now: i64,
    ) -> Result<Vec<AccountClaim>, StoreError> {
        assert!(!platforms.is_empty(), "platforms must not be empty");
        assert!(limit > 0, "limit must be positive");

        let claims = sqlx::query_as::<_, AccountClaim>(
            "WITH claimed AS ( \
                 SELECT puuid FROM account \
                 WHERE status = 'pending' AND platform = ANY($1) \
                 ORDER BY priority ASC, discovered_at ASC \
                 LIMIT $2 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE account \
             SET status = 'claimed', crawled_at = $3 \
             FROM claimed \
             WHERE account.puuid = claimed.puuid \
             RETURNING account.puuid, account.platform",
        )
        .bind(platforms)
        .bind(limit)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        Ok(claims)
    }

    pub async fn mark_account_done(&self, puuid: &str, now: i64) -> Result<(), StoreError> {
        assert!(!puuid.is_empty(), "puuid must not be empty");

        sqlx::query("UPDATE account SET status = 'done', crawled_at = $1 WHERE puuid = $2")
            .bind(now)
            .bind(puuid)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn mark_matches_seen(
        &self,
        match_ids: &[String],
        platform: &str,
        now: i64,
    ) -> Result<Vec<String>, StoreError> {
        assert!(!platform.is_empty(), "platform must not be empty");
        assert!(
            match_ids.len() <= BATCH_TOTAL_MAX,
            "too many match ids in one batch"
        );

        let mut new_ids: Vec<String> = Vec::with_capacity(match_ids.len());

        for chunk in match_ids.chunks(BATCH_CHUNK_MAX) {
            let inserted = sqlx::query_scalar::<_, String>(
                "INSERT INTO match_seen (match_id, platform, status, discovered_at) \
                 SELECT match_id, $2, 'pending', $3 FROM unnest($1::text[]) AS t(match_id) \
                 ON CONFLICT DO NOTHING \
                 RETURNING match_id",
            )
            .bind(chunk)
            .bind(platform)
            .bind(now)
            .fetch_all(&self.pool)
            .await?;

            new_ids.extend(inserted);
        }

        Ok(new_ids)
    }

    pub async fn mark_match_skipped(&self, match_id: &str) -> Result<(), StoreError> {
        assert!(!match_id.is_empty(), "match id must not be empty");

        sqlx::query("UPDATE match_seen SET status = 'skipped' WHERE match_id = $1")
            .bind(match_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn mark_match_retry(
        &self,
        match_id: &str,
        attempts_max: i32,
    ) -> Result<bool, StoreError> {
        assert!(!match_id.is_empty(), "match id must not be empty");
        assert!(attempts_max > 0, "attempts max must be positive");

        // The row stays 'pending' so the recovery sweep retries it; the CASE
        // flips it to 'skipped' once the attempt bound trips.
        let skipped = sqlx::query_scalar::<_, bool>(
            "UPDATE match_seen \
             SET attempts = attempts + 1, \
                 status = CASE WHEN attempts + 1 >= $2 THEN 'skipped' ELSE 'pending' END \
             WHERE match_id = $1 \
             RETURNING status = 'skipped'",
        )
        .bind(match_id)
        .bind(attempts_max)
        .fetch_optional(&self.pool)
        .await?;

        Ok(skipped.unwrap_or(false))
    }

    pub async fn store_match(
        &self,
        record: &MatchRecord,
        participants: &[ParticipantRecord],
    ) -> Result<(), StoreError> {
        assert!(!record.match_id.is_empty(), "match id must not be empty");
        assert!(
            participants.len() <= PARTICIPANTS_PER_MATCH_MAX,
            "too many participants for one match"
        );

        let mut transaction = self.pool.begin().await?;

        insert_match(&mut transaction, record).await?;

        if !participants.is_empty() {
            insert_participants(&mut transaction, participants).await?;
        }

        sqlx::query("UPDATE match_seen SET status = 'fetched' WHERE match_id = $1")
            .bind(&record.match_id)
            .execute(&mut *transaction)
            .await?;

        upsert_queue_catalog(&mut transaction, record).await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn store_timeline(
        &self,
        records: &[ParticipantTimelineRecord],
    ) -> Result<(), StoreError> {
        assert!(!records.is_empty(), "timeline records must not be empty");
        assert!(
            records.len() <= PARTICIPANTS_PER_MATCH_MAX,
            "too many timeline records for one match"
        );

        let mut transaction = self.pool.begin().await?;

        insert_timelines(&mut transaction, records).await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn rebuild_stats(&self, now: i64, patch: Option<&str>) -> Result<(), StoreError> {
        if let Some(patch) = patch {
            assert!(!patch.is_empty(), "patch must not be empty");
        }

        let mut transaction = self.pool.begin().await?;

        clear_rollups(&mut transaction, patch).await?;
        rebuild_champion_stat(&mut transaction, now, patch).await?;
        rebuild_queue_totals(&mut transaction, now, patch).await?;
        rebuild_item_stat(&mut transaction, now, patch).await?;
        rebuild_keystone_stat(&mut transaction, now, patch).await?;
        rebuild_spell_pair_stat(&mut transaction, now, patch).await?;
        rebuild_rune_style_stat(&mut transaction, now, patch).await?;
        rebuild_rune_page_stat(&mut transaction, now, patch).await?;
        rebuild_skill_order_stat(&mut transaction, now, patch).await?;
        rebuild_item_start_stat(&mut transaction, now, patch).await?;
        rebuild_item_core_stat(&mut transaction, now, patch).await?;
        rebuild_boot_stat(&mut transaction, now, patch).await?;
        rebuild_skill_sequence_stat(&mut transaction, now, patch).await?;
        rebuild_matchup_stat(&mut transaction, now, patch).await?;
        rebuild_synergy_stat(&mut transaction, now, patch).await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn champion_stats(
        &self,
        patch: &str,
        queue_id: i32,
    ) -> Result<Vec<ChampionStat>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");

        let stats = sqlx::query_as::<_, ChampionStat>(
            "SELECT patch, queue_id, champion_id, champion_name, games, wins, kills_sum, \
                    deaths_sum, assists_sum, damage_sum, gold_sum \
             FROM champion_stat \
             WHERE patch = $1 AND queue_id = $2 \
             ORDER BY games DESC",
        )
        .bind(patch)
        .bind(queue_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(stats)
    }

    pub async fn champion_stats_ranked(
        &self,
        patch: &str,
        queue_id: i32,
        games_min: i64,
    ) -> Result<Vec<ChampionStat>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");

        let stats = sqlx::query_as::<_, ChampionStat>(
            "SELECT patch, queue_id, champion_id, champion_name, games, wins, kills_sum, \
                    deaths_sum, assists_sum, damage_sum, gold_sum \
             FROM champion_stat \
             WHERE patch = $1 AND queue_id = $2 AND games >= $3 \
             ORDER BY (wins::float8 / games) DESC, games DESC",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(games_min)
        .fetch_all(&self.pool)
        .await?;

        Ok(stats)
    }

    pub async fn queue_total(&self, patch: &str, queue_id: i32) -> Result<Option<i64>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");

        let total = sqlx::query_scalar::<_, i64>(
            "SELECT games FROM queue_totals WHERE patch = $1 AND queue_id = $2",
        )
        .bind(patch)
        .bind(queue_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(total)
    }

    pub async fn queue_catalog(&self) -> Result<Vec<QueueCatalogEntry>, StoreError> {
        let entries = sqlx::query_as::<_, QueueCatalogEntry>(
            "SELECT queue_id, game_mode, mutators, games, first_seen, last_seen \
             FROM queue_catalog \
             ORDER BY games DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(entries)
    }

    pub async fn patches(&self) -> Result<Vec<String>, StoreError> {
        let patches = sqlx::query_scalar::<_, String>(
            "SELECT patch FROM (SELECT DISTINCT patch FROM queue_totals) AS distinct_patch \
             ORDER BY string_to_array(patch, '.')::int[] DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(patches)
    }

    pub async fn patches_crawled(&self) -> Result<Vec<String>, StoreError> {
        let patches = sqlx::query_scalar::<_, String>(
            "SELECT patch FROM (SELECT DISTINCT patch FROM match_record) AS distinct_patch \
             ORDER BY string_to_array(patch, '.')::int[] DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(patches)
    }

    pub async fn counts(&self) -> Result<CrawlStats, StoreError> {
        let stats = sqlx::query_as::<_, CrawlStats>(
            "SELECT \
                 (SELECT COUNT(*) FROM account WHERE status <> 'done') AS accounts_pending, \
                 (SELECT COUNT(*) FROM account WHERE status = 'done') AS accounts_done, \
                 (SELECT COUNT(*) FROM match_record) AS matches_stored, \
                 (SELECT COUNT(*) FROM participant) AS participants_stored",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(stats)
    }

    pub async fn champion_items(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<IdWinCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, IdWinCount>(
            "SELECT item_id AS id, games, wins \
             FROM item_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_keystones(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<IdWinCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, IdWinCount>(
            "SELECT perk_keystone AS id, games, wins \
             FROM keystone_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_spell_pairs(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<SpellPairCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, SpellPairCount>(
            "SELECT spell_a, spell_b, games, wins \
             FROM spell_pair_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_rune_styles(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<StylePairCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, StylePairCount>(
            "SELECT primary_style, sub_style, games, wins \
             FROM rune_style_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_skill_orders(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<SkillOrderCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, SkillOrderCount>(
            "SELECT skill_max_order, games, wins \
             FROM skill_order_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_item_starts(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<LabeledCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, LabeledCount>(
            "SELECT item_start AS label, games, wins \
             FROM item_start_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_item_cores(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<LabeledCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, LabeledCount>(
            "SELECT item_core AS label, games, wins \
             FROM item_core_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_boots(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<IdWinCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, IdWinCount>(
            "SELECT boots AS id, games, wins \
             FROM boot_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_skill_sequences(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<LabeledCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, LabeledCount>(
            "SELECT skill_sequence AS label, games, wins \
             FROM skill_sequence_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_matchups(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        games_min: i64,
        limit: i64,
    ) -> Result<Vec<IdWinCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, IdWinCount>(
            "SELECT opponent_id AS id, games, wins \
             FROM matchup_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 AND games >= $4 \
             ORDER BY games DESC \
             LIMIT $5",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(games_min)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_synergies(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        games_min: i64,
        limit: i64,
    ) -> Result<Vec<IdWinCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, IdWinCount>(
            "SELECT ally_id AS id, games, wins \
             FROM synergy_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 AND games >= $4 \
             ORDER BY games DESC \
             LIMIT $5",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(games_min)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn champion_rune_pages(
        &self,
        patch: &str,
        queue_id: i32,
        champion_id: i32,
        limit: i64,
    ) -> Result<Vec<RunePageCount>, StoreError> {
        assert!(!patch.is_empty(), "patch must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, RunePageCount>(
            "SELECT perk_primary_csv, perk_sub_csv, perk_shard_csv, perk_primary_style, \
                    perk_sub_style, games, wins \
             FROM rune_page_stat \
             WHERE patch = $1 AND queue_id = $2 AND champion_id = $3 \
             ORDER BY games DESC \
             LIMIT $4",
        )
        .bind(patch)
        .bind(queue_id)
        .bind(champion_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn requeue_account(
        &self,
        puuid: &str,
        platform: &str,
        priority: i32,
        now: i64,
    ) -> Result<bool, StoreError> {
        assert!(!puuid.is_empty(), "puuid must not be empty");
        assert!(!platform.is_empty(), "platform must not be empty");

        let result = sqlx::query(
            "INSERT INTO account (puuid, platform, priority, status, discovered_at) \
             VALUES ($1, $2, $3, 'pending', $4) \
             ON CONFLICT (puuid) DO UPDATE \
             SET status = 'pending', priority = LEAST(account.priority, EXCLUDED.priority) \
             WHERE account.status = 'done'",
        )
        .bind(puuid)
        .bind(platform)
        .bind(priority)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn revisit_accounts(&self, cutoff: i64, limit: i64) -> Result<u64, StoreError> {
        assert!(cutoff > 0, "cutoff must be positive");
        assert!(limit > 0, "limit must be positive");

        let updated = sqlx::query(
            "UPDATE account SET status = 'pending' \
             WHERE puuid IN ( \
                 SELECT puuid FROM account \
                 WHERE status = 'done' AND crawled_at < $1 \
                 ORDER BY crawled_at ASC \
                 LIMIT $2 \
             )",
        )
        .bind(cutoff)
        .bind(limit)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected())
    }

    pub async fn stale_pending_matches(
        &self,
        cutoff: i64,
        limit: i64,
    ) -> Result<Vec<MatchPending>, StoreError> {
        assert!(cutoff > 0, "cutoff must be positive");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, MatchPending>(
            "SELECT match_id, platform FROM match_seen \
             WHERE status = 'pending' AND discovered_at < $1 \
             ORDER BY discovered_at ASC \
             LIMIT $2",
        )
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn patches_fetched_since(
        &self,
        since: i64,
    ) -> Result<Vec<(String, i64)>, StoreError> {
        assert!(since > 0, "since must be positive");

        let patches = sqlx::query_as::<_, (String, i64)>(
            "SELECT patch, COUNT(*) FROM match_record WHERE fetched_at >= $1 GROUP BY patch",
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        Ok(patches)
    }

    pub async fn counts_estimated(&self) -> Result<CrawlStats, StoreError> {
        let stats = sqlx::query_as::<_, CrawlStats>(
            "SELECT \
                 (SELECT COUNT(*) FROM account WHERE status = 'pending') AS accounts_pending, \
                 (SELECT COUNT(*) FROM account WHERE status = 'done') AS accounts_done, \
                 (SELECT GREATEST(reltuples::bigint, 0) FROM pg_class \
                  WHERE relname = 'match_record') AS matches_stored, \
                 (SELECT GREATEST(reltuples::bigint, 0) FROM pg_class \
                  WHERE relname = 'participant') AS participants_stored",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(stats)
    }

    pub async fn player_champion_stats(
        &self,
        puuid: &str,
        limit: i64,
    ) -> Result<Vec<PlayerChampionStat>, StoreError> {
        assert!(!puuid.is_empty(), "puuid must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, PlayerChampionStat>(
            "SELECT champion_id, MAX(champion_name) AS champion_name, COUNT(*) AS games, \
                    COUNT(*) FILTER (WHERE win) AS wins, SUM(kills)::bigint AS kills_sum, \
                    SUM(deaths)::bigint AS deaths_sum, SUM(assists)::bigint AS assists_sum \
             FROM participant \
             WHERE puuid = $1 \
             GROUP BY champion_id \
             ORDER BY games DESC \
             LIMIT $2",
        )
        .bind(puuid)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn match_meta(&self, match_id: &str) -> Result<Option<MatchMeta>, StoreError> {
        assert!(!match_id.is_empty(), "match id must not be empty");

        let meta = sqlx::query_as::<_, MatchMeta>(
            "SELECT match_id, patch, queue_id, game_mode, game_creation, game_duration \
             FROM match_record \
             WHERE match_id = $1",
        )
        .bind(match_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(meta)
    }

    pub async fn match_participants(
        &self,
        match_id: &str,
    ) -> Result<Vec<MatchParticipant>, StoreError> {
        assert!(!match_id.is_empty(), "match id must not be empty");

        let rows = sqlx::query_as::<_, MatchParticipant>(
            "SELECT puuid, champion_id, champion_name, team_id, win, kills, deaths, assists, \
                    champion_level, gold_earned, damage_to_champions, item0, item1, item2, item3, \
                    item4, item5, item6, summoner1_id, summoner2_id, perk_keystone, \
                    perk_primary_style, perk_sub_style \
             FROM participant \
             WHERE match_id = $1 \
             ORDER BY team_id, puuid",
        )
        .bind(match_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn player_matches(
        &self,
        puuid: &str,
        limit: i64,
    ) -> Result<Vec<PlayerMatch>, StoreError> {
        assert!(!puuid.is_empty(), "puuid must not be empty");
        assert!(limit > 0, "limit must be positive");

        let rows = sqlx::query_as::<_, PlayerMatch>(
            "SELECT p.match_id, p.patch, p.queue_id, p.champion_id, p.champion_name, p.win, \
                    p.kills, p.deaths, p.assists, m.game_creation \
             FROM participant p \
             JOIN match_record m ON m.match_id = p.match_id \
             WHERE p.puuid = $1 \
             ORDER BY m.game_creation DESC \
             LIMIT $2",
        )
        .bind(puuid)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }
}

async fn insert_match(
    transaction: &mut Transaction,
    record: &MatchRecord,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO match_record \
         (match_id, platform, patch, queue_id, game_mode, game_mutators, game_version, \
          game_creation, game_duration, fetched_at, raw_json) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&record.match_id)
    .bind(&record.platform)
    .bind(&record.patch)
    .bind(record.queue_id)
    .bind(&record.game_mode)
    .bind(&record.game_mutators)
    .bind(&record.game_version)
    .bind(record.game_creation)
    .bind(record.game_duration)
    .bind(record.fetched_at)
    .bind(&record.raw_json)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn insert_participants(
    transaction: &mut Transaction,
    participants: &[ParticipantRecord],
) -> Result<(), StoreError> {
    assert!(!participants.is_empty(), "participants must not be empty");
    assert!(
        participants.len() <= PARTICIPANTS_PER_MATCH_MAX,
        "too many participants for one insert"
    );

    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO participant \
         (match_id, puuid, patch, champion_name, team_position, queue_id, champion_id, team_id, \
          kills, deaths, assists, champion_level, gold_earned, damage_to_champions, item0, item1, \
          item2, item3, item4, item5, item6, summoner1_id, summoner2_id, perk_keystone, \
          perk_primary_style, perk_sub_style, perk_primary_csv, perk_sub_csv, perk_shard_csv, \
          win) ",
    );

    builder.push_values(participants, |mut row, participant| {
        row.push_bind(&participant.match_id)
            .push_bind(&participant.puuid)
            .push_bind(&participant.patch)
            .push_bind(&participant.champion_name)
            .push_bind(&participant.team_position)
            .push_bind(participant.queue_id)
            .push_bind(participant.champion_id)
            .push_bind(participant.team_id)
            .push_bind(participant.kills)
            .push_bind(participant.deaths)
            .push_bind(participant.assists)
            .push_bind(participant.champion_level)
            .push_bind(participant.gold_earned)
            .push_bind(participant.damage_to_champions)
            .push_bind(participant.item0)
            .push_bind(participant.item1)
            .push_bind(participant.item2)
            .push_bind(participant.item3)
            .push_bind(participant.item4)
            .push_bind(participant.item5)
            .push_bind(participant.item6)
            .push_bind(participant.summoner1_id)
            .push_bind(participant.summoner2_id)
            .push_bind(participant.perk_keystone)
            .push_bind(participant.perk_primary_style)
            .push_bind(participant.perk_sub_style)
            .push_bind(&participant.perk_primary_csv)
            .push_bind(&participant.perk_sub_csv)
            .push_bind(&participant.perk_shard_csv)
            .push_bind(participant.win);
    });

    builder.push(" ON CONFLICT DO NOTHING");

    builder.build().execute(&mut **transaction).await?;

    Ok(())
}

async fn insert_timelines(
    transaction: &mut Transaction,
    records: &[ParticipantTimelineRecord],
) -> Result<(), StoreError> {
    assert!(!records.is_empty(), "timeline records must not be empty");
    assert!(
        records.len() <= PARTICIPANTS_PER_MATCH_MAX,
        "too many timeline records for one insert"
    );

    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO participant_timeline \
         (match_id, puuid, patch, queue_id, champion_id, win, skill_order, skill_max_order, \
          skill_sequence, item_order, item_start, item_core, boots) ",
    );

    builder.push_values(records, |mut row, record| {
        row.push_bind(&record.match_id)
            .push_bind(&record.puuid)
            .push_bind(&record.patch)
            .push_bind(record.queue_id)
            .push_bind(record.champion_id)
            .push_bind(record.win)
            .push_bind(&record.skill_order)
            .push_bind(&record.skill_max_order)
            .push_bind(&record.skill_sequence)
            .push_bind(&record.item_order)
            .push_bind(&record.item_start)
            .push_bind(&record.item_core)
            .push_bind(record.boots);
    });

    builder.push(" ON CONFLICT DO NOTHING");

    builder.build().execute(&mut **transaction).await?;

    Ok(())
}

async fn upsert_queue_catalog(
    transaction: &mut Transaction,
    record: &MatchRecord,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO queue_catalog (queue_id, game_mode, mutators, games, first_seen, last_seen) \
         VALUES ($1, $2, $3, 1, $4, $5) \
         ON CONFLICT (queue_id, game_mode, mutators) \
         DO UPDATE SET games = queue_catalog.games + 1, last_seen = EXCLUDED.last_seen",
    )
    .bind(record.queue_id)
    .bind(&record.game_mode)
    .bind(&record.game_mutators)
    .bind(record.fetched_at)
    .bind(record.fetched_at)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn clear_rollups(
    transaction: &mut Transaction,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let tables = [
        "champion_stat",
        "queue_totals",
        "item_stat",
        "keystone_stat",
        "spell_pair_stat",
        "rune_style_stat",
        "rune_page_stat",
        "skill_order_stat",
        "item_start_stat",
        "item_core_stat",
        "boot_stat",
        "skill_sequence_stat",
        "matchup_stat",
        "synergy_stat",
    ];

    for table in tables {
        let sql = match patch {
            Some(_) => format!("DELETE FROM {table} WHERE patch = $1"),
            None => format!("DELETE FROM {table}"),
        };

        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));

        if let Some(patch) = patch {
            query = query.bind(patch);
        }

        query.execute(&mut **transaction).await?;
    }

    Ok(())
}

async fn execute_rollup(
    transaction: &mut Transaction,
    sql: &str,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(now);

    if let Some(patch) = patch {
        query = query.bind(patch);
    }

    query.execute(&mut **transaction).await?;

    Ok(())
}

async fn rebuild_champion_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "WHERE patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO champion_stat \
         (patch, queue_id, champion_id, champion_name, games, wins, kills_sum, deaths_sum, \
          assists_sum, damage_sum, gold_sum, computed_at) \
         SELECT patch, queue_id, champion_id, MAX(champion_name), COUNT(*), \
                COUNT(*) FILTER (WHERE win), SUM(kills)::bigint, SUM(deaths)::bigint, \
                SUM(assists)::bigint, SUM(damage_to_champions)::bigint, \
                SUM(gold_earned)::bigint, $1 \
         FROM participant \
         {filter}GROUP BY patch, queue_id, champion_id"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_queue_totals(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "WHERE patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO queue_totals (patch, queue_id, games, computed_at) \
         SELECT patch, queue_id, COUNT(*), $1 FROM match_record \
         {filter}GROUP BY patch, queue_id"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_item_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND p.patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO item_stat (patch, queue_id, champion_id, item_id, games, wins, computed_at) \
         SELECT p.patch, p.queue_id, p.champion_id, v.item, COUNT(*), \
                COUNT(*) FILTER (WHERE p.win), $1 \
         FROM participant p, \
              LATERAL (VALUES (p.item0), (p.item1), (p.item2), (p.item3), (p.item4), \
                              (p.item5), (p.item6)) AS v(item) \
         WHERE v.item <> 0 \
         {filter}GROUP BY p.patch, p.queue_id, p.champion_id, v.item"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_keystone_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO keystone_stat \
         (patch, queue_id, champion_id, perk_keystone, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, perk_keystone, COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant \
         WHERE perk_keystone <> 0 \
         {filter}GROUP BY patch, queue_id, champion_id, perk_keystone"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_spell_pair_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO spell_pair_stat \
         (patch, queue_id, champion_id, spell_a, spell_b, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, LEAST(summoner1_id, summoner2_id), \
                GREATEST(summoner1_id, summoner2_id), COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant \
         WHERE summoner1_id <> 0 AND summoner2_id <> 0 \
         {filter}GROUP BY patch, queue_id, champion_id, LEAST(summoner1_id, summoner2_id), \
                  GREATEST(summoner1_id, summoner2_id)"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_rune_style_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO rune_style_stat \
         (patch, queue_id, champion_id, primary_style, sub_style, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, perk_primary_style, perk_sub_style, COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant \
         WHERE perk_primary_style <> 0 \
         {filter}GROUP BY patch, queue_id, champion_id, perk_primary_style, perk_sub_style"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_skill_order_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO skill_order_stat \
         (patch, queue_id, champion_id, skill_max_order, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, skill_max_order, COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant_timeline \
         WHERE skill_max_order <> '' \
         {filter}GROUP BY patch, queue_id, champion_id, skill_max_order"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_item_start_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO item_start_stat \
         (patch, queue_id, champion_id, item_start, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, item_start, COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant_timeline \
         WHERE item_start <> '' \
         {filter}GROUP BY patch, queue_id, champion_id, item_start"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_item_core_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO item_core_stat \
         (patch, queue_id, champion_id, item_core, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, item_core, COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant_timeline \
         WHERE item_core <> '' \
         {filter}GROUP BY patch, queue_id, champion_id, item_core"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_boot_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO boot_stat \
         (patch, queue_id, champion_id, boots, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, boots, COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant_timeline \
         WHERE boots <> 0 \
         {filter}GROUP BY patch, queue_id, champion_id, boots"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_skill_sequence_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO skill_sequence_stat \
         (patch, queue_id, champion_id, skill_sequence, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, skill_sequence, COUNT(*), \
                COUNT(*) FILTER (WHERE win), $1 \
         FROM participant_timeline \
         WHERE skill_sequence <> '' \
         {filter}GROUP BY patch, queue_id, champion_id, skill_sequence"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_rune_page_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "AND patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO rune_page_stat \
         (patch, queue_id, champion_id, perk_primary_csv, perk_sub_csv, perk_shard_csv, \
          perk_primary_style, perk_sub_style, games, wins, computed_at) \
         SELECT patch, queue_id, champion_id, perk_primary_csv, perk_sub_csv, perk_shard_csv, \
                MIN(perk_primary_style), MIN(perk_sub_style), \
                COUNT(*), COUNT(*) FILTER (WHERE win), $1 \
         FROM participant \
         WHERE perk_primary_csv <> '' \
         {filter}GROUP BY patch, queue_id, champion_id, perk_primary_csv, perk_sub_csv, \
                  perk_shard_csv"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_matchup_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "WHERE a.patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO matchup_stat \
         (patch, queue_id, champion_id, opponent_id, games, wins, computed_at) \
         SELECT a.patch, a.queue_id, a.champion_id, b.champion_id, COUNT(*), \
                COUNT(*) FILTER (WHERE a.win), $1 \
         FROM participant a \
         JOIN participant b ON b.match_id = a.match_id AND b.team_id <> a.team_id \
         {filter}GROUP BY a.patch, a.queue_id, a.champion_id, b.champion_id"
    );

    execute_rollup(transaction, &sql, now, patch).await
}

async fn rebuild_synergy_stat(
    transaction: &mut Transaction,
    now: i64,
    patch: Option<&str>,
) -> Result<(), StoreError> {
    let filter = if patch.is_some() {
        "WHERE a.patch = $2 "
    } else {
        ""
    };

    let sql = format!(
        "INSERT INTO synergy_stat \
         (patch, queue_id, champion_id, ally_id, games, wins, computed_at) \
         SELECT a.patch, a.queue_id, a.champion_id, b.champion_id, COUNT(*), \
                COUNT(*) FILTER (WHERE a.win), $1 \
         FROM participant a \
         JOIN participant b ON b.match_id = a.match_id AND b.team_id = a.team_id \
                           AND b.puuid <> a.puuid \
         {filter}GROUP BY a.patch, a.queue_id, a.champion_id, b.champion_id"
    );

    execute_rollup(transaction, &sql, now, patch).await
}
