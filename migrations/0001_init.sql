-- Consolidated schema, squashed from the pre-release migration history on
-- 2026-07-13. Matches the live schema at that point exactly.

-- The account frontier: the breadth-first queue of puuids to crawl. A puuid is
-- inserted once (the primary key doubles as the seen set) and moves pending ->
-- claimed -> done. Lower priority is crawled sooner. crawled_at is stamped at
-- claim time, so stale claims from dead workers can be reaped back to pending.
CREATE TABLE IF NOT EXISTS account (
    puuid         TEXT   NOT NULL PRIMARY KEY,
    platform      TEXT   NOT NULL,
    priority      INT    NOT NULL,
    status        TEXT   NOT NULL DEFAULT 'pending',
    discovered_at BIGINT NOT NULL,
    crawled_at    BIGINT
);

-- Partial index over the pending frontier only: claimed and done rows never
-- pollute it. Platform trails the sort keys so the ordered claim scan filters
-- platforms in-index instead of via heap visits.
CREATE INDEX IF NOT EXISTS account_claim_pending
    ON account (priority, discovered_at, platform)
    WHERE status = 'pending';

-- Revisit sweep: done accounts whose last crawl is older than the TTL flip
-- back to pending so new games by known players are picked up.
CREATE INDEX IF NOT EXISTS account_revisit_done
    ON account (crawled_at)
    WHERE status = 'done';

-- The match dedup ledger. Insert-or-ignore on the primary key answers "is this
-- match new" without a separate seen set. Retriable fetch failures (429
-- storms, transient 5xx, transport faults) keep rows 'pending' so the recovery
-- sweep retries them; attempts bounds the retries before the row is skipped.
CREATE TABLE IF NOT EXISTS match_seen (
    match_id      TEXT   NOT NULL PRIMARY KEY,
    platform      TEXT   NOT NULL,
    status        TEXT   NOT NULL DEFAULT 'pending',
    discovered_at BIGINT NOT NULL,
    attempts      INT    NOT NULL DEFAULT 0
);

-- Orphan recovery: matches marked seen but never fetched (crash between the
-- dedup insert and the fetch) are retried at startup and periodically.
CREATE INDEX IF NOT EXISTS match_seen_pending
    ON match_seen (discovered_at)
    WHERE status = 'pending';

-- Stored matches. The raw json is retained so participants can be re-derived if
-- the flattened columns ever change. Patch is game_version major.minor. Named
-- match_record because match is a reserved keyword in Postgres. raw_json is
-- TOASTed with lz4, which compresses and decompresses far faster than pglz.
CREATE TABLE IF NOT EXISTS match_record (
    match_id      TEXT   NOT NULL PRIMARY KEY,
    platform      TEXT   NOT NULL,
    patch         TEXT   NOT NULL,
    queue_id      INT    NOT NULL,
    game_mode     TEXT   NOT NULL,
    game_mutators TEXT   NOT NULL DEFAULT '',
    game_version  TEXT   NOT NULL,
    game_creation BIGINT NOT NULL,
    game_duration BIGINT NOT NULL,
    fetched_at    BIGINT NOT NULL,
    raw_json      TEXT   COMPRESSION lz4 NOT NULL
);

CREATE INDEX IF NOT EXISTS match_record_patch_queue
    ON match_record (patch, queue_id);

-- The service aggregates only patches that received matches since the last
-- tick; this index makes that recency probe cheap.
CREATE INDEX IF NOT EXISTS match_record_fetched_at
    ON match_record (fetched_at);

-- Flattened participant rows, ten per stored match, feeding aggregation. The
-- item, spell, and rune columns are the final build; skill order needs the
-- timeline and is not captured here. Fixed-width columns sit together after the
-- text keys to minimize alignment padding. The perk CSVs are the full rune
-- page: primary-tree and sub-tree rune ids in selection order plus the three
-- stat shards as offense,flex,defense, each a comma-separated id list.
CREATE TABLE IF NOT EXISTS participant (
    match_id            TEXT    NOT NULL,
    puuid               TEXT    NOT NULL,
    patch               TEXT    NOT NULL,
    champion_name       TEXT    NOT NULL,
    team_position       TEXT    NOT NULL DEFAULT '',
    queue_id            INT     NOT NULL,
    champion_id         INT     NOT NULL,
    team_id             INT     NOT NULL,
    kills               INT     NOT NULL,
    deaths              INT     NOT NULL,
    assists             INT     NOT NULL,
    champion_level      INT     NOT NULL DEFAULT 0,
    gold_earned         INT     NOT NULL DEFAULT 0,
    damage_to_champions INT     NOT NULL DEFAULT 0,
    item0               INT     NOT NULL DEFAULT 0,
    item1               INT     NOT NULL DEFAULT 0,
    item2               INT     NOT NULL DEFAULT 0,
    item3               INT     NOT NULL DEFAULT 0,
    item4               INT     NOT NULL DEFAULT 0,
    item5               INT     NOT NULL DEFAULT 0,
    item6               INT     NOT NULL DEFAULT 0,
    summoner1_id        INT     NOT NULL DEFAULT 0,
    summoner2_id        INT     NOT NULL DEFAULT 0,
    perk_keystone       INT     NOT NULL DEFAULT 0,
    perk_primary_style  INT     NOT NULL DEFAULT 0,
    perk_sub_style      INT     NOT NULL DEFAULT 0,
    win                 BOOLEAN NOT NULL,
    perk_primary_csv    TEXT    NOT NULL DEFAULT '',
    perk_sub_csv        TEXT    NOT NULL DEFAULT '',
    perk_shard_csv      TEXT    NOT NULL DEFAULT '',
    PRIMARY KEY (match_id, puuid)
);

CREATE INDEX IF NOT EXISTS participant_rollup
    ON participant (patch, queue_id, champion_id);

-- Player profile endpoints filter by puuid; the primary key leads with
-- match_id, so those lookups need their own index.
CREATE INDEX IF NOT EXISTS participant_puuid
    ON participant (puuid);

-- Per-participant derived timeline: skill level order and item purchase order,
-- extracted from the match-v5 timeline. The raw timeline is not retained; only
-- these compact sequences are. skill_max_order is the maxed priority such as
-- "Q>E>W". item_start is the sorted starting purchases, item_core is the
-- ordered completed items, boots is the boots item id, and skill_sequence is
-- the first several skill slots leveled. Populated only when timeline fetching
-- is enabled.
CREATE TABLE IF NOT EXISTS participant_timeline (
    match_id        TEXT    NOT NULL,
    puuid           TEXT    NOT NULL,
    patch           TEXT    NOT NULL,
    queue_id        INT     NOT NULL,
    champion_id     INT     NOT NULL,
    win             BOOLEAN NOT NULL,
    skill_order     TEXT    NOT NULL DEFAULT '',
    skill_max_order TEXT    NOT NULL DEFAULT '',
    item_order      TEXT    NOT NULL DEFAULT '',
    item_start      TEXT    NOT NULL DEFAULT '',
    item_core       TEXT    NOT NULL DEFAULT '',
    boots           INT     NOT NULL DEFAULT 0,
    skill_sequence  TEXT    NOT NULL DEFAULT '',
    PRIMARY KEY (match_id, puuid)
);

CREATE INDEX IF NOT EXISTS participant_timeline_rollup
    ON participant_timeline (patch, queue_id, champion_id);

-- Empirically discovered queue and mode combinations. This is how ARAM Mayhem,
-- which has no published queue id, is identified: an unfamiliar (queue_id,
-- game_mode, mutators) tuple accrues games here.
CREATE TABLE IF NOT EXISTS queue_catalog (
    queue_id   INT    NOT NULL,
    game_mode  TEXT   NOT NULL,
    mutators   TEXT   NOT NULL DEFAULT '',
    games      BIGINT NOT NULL,
    first_seen BIGINT NOT NULL,
    last_seen  BIGINT NOT NULL,
    PRIMARY KEY (queue_id, game_mode, mutators)
);

-- Rollup tables below are rebuilt by the aggregate step from participant and
-- participant_timeline. Rates are derived by readers from these raw sums.

-- Champion statistics per patch and queue.
CREATE TABLE IF NOT EXISTS champion_stat (
    patch         TEXT   NOT NULL,
    queue_id      INT    NOT NULL,
    champion_id   INT    NOT NULL,
    champion_name TEXT   NOT NULL,
    games         BIGINT NOT NULL,
    wins          BIGINT NOT NULL,
    kills_sum     BIGINT NOT NULL,
    deaths_sum    BIGINT NOT NULL,
    assists_sum   BIGINT NOT NULL,
    computed_at   BIGINT NOT NULL,
    damage_sum    BIGINT NOT NULL DEFAULT 0,
    gold_sum      BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (patch, queue_id, champion_id)
);

-- Total games per patch and queue, so readers can compute pick rate. Rebuilt
-- alongside champion_stat.
CREATE TABLE IF NOT EXISTS queue_totals (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    games       BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id)
);

CREATE TABLE IF NOT EXISTS item_stat (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    champion_id INT    NOT NULL,
    item_id     INT    NOT NULL,
    games       BIGINT NOT NULL,
    wins        BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, item_id)
);

CREATE TABLE IF NOT EXISTS keystone_stat (
    patch         TEXT   NOT NULL,
    queue_id      INT    NOT NULL,
    champion_id   INT    NOT NULL,
    perk_keystone INT    NOT NULL,
    games         BIGINT NOT NULL,
    wins          BIGINT NOT NULL,
    computed_at   BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, perk_keystone)
);

CREATE TABLE IF NOT EXISTS spell_pair_stat (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    champion_id INT    NOT NULL,
    spell_a     INT    NOT NULL,
    spell_b     INT    NOT NULL,
    games       BIGINT NOT NULL,
    wins        BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, spell_a, spell_b)
);

CREATE TABLE IF NOT EXISTS rune_style_stat (
    patch          TEXT   NOT NULL,
    queue_id       INT    NOT NULL,
    champion_id    INT    NOT NULL,
    primary_style  INT    NOT NULL,
    sub_style      INT    NOT NULL,
    games          BIGINT NOT NULL,
    wins           BIGINT NOT NULL,
    computed_at    BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, primary_style, sub_style)
);

CREATE TABLE IF NOT EXISTS skill_order_stat (
    patch           TEXT   NOT NULL,
    queue_id        INT    NOT NULL,
    champion_id     INT    NOT NULL,
    skill_max_order TEXT   NOT NULL,
    games           BIGINT NOT NULL,
    wins            BIGINT NOT NULL,
    computed_at     BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, skill_max_order)
);

CREATE TABLE IF NOT EXISTS item_start_stat (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    champion_id INT    NOT NULL,
    item_start  TEXT   NOT NULL,
    games       BIGINT NOT NULL,
    wins        BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, item_start)
);

CREATE TABLE IF NOT EXISTS item_core_stat (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    champion_id INT    NOT NULL,
    item_core   TEXT   NOT NULL,
    games       BIGINT NOT NULL,
    wins        BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, item_core)
);

CREATE TABLE IF NOT EXISTS boot_stat (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    champion_id INT    NOT NULL,
    boots       INT    NOT NULL,
    games       BIGINT NOT NULL,
    wins        BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, boots)
);

CREATE TABLE IF NOT EXISTS skill_sequence_stat (
    patch          TEXT   NOT NULL,
    queue_id       INT    NOT NULL,
    champion_id    INT    NOT NULL,
    skill_sequence TEXT   NOT NULL,
    games          BIGINT NOT NULL,
    wins           BIGINT NOT NULL,
    computed_at    BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, skill_sequence)
);

-- Directed champion-versus-champion results per patch and queue: games and
-- wins for champion_id when opponent_id was on the enemy team. Rebuilt by the
-- aggregate step from a participant self-join.
CREATE TABLE IF NOT EXISTS matchup_stat (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    champion_id INT    NOT NULL,
    opponent_id INT    NOT NULL,
    games       BIGINT NOT NULL,
    wins        BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, opponent_id)
);

-- Directed same-team pairings per patch and queue: games and wins for
-- champion_id when ally_id was on the same team.
CREATE TABLE IF NOT EXISTS synergy_stat (
    patch       TEXT   NOT NULL,
    queue_id    INT    NOT NULL,
    champion_id INT    NOT NULL,
    ally_id     INT    NOT NULL,
    games       BIGINT NOT NULL,
    wins        BIGINT NOT NULL,
    computed_at BIGINT NOT NULL,
    PRIMARY KEY (patch, queue_id, champion_id, ally_id)
);

-- Full rune page popularity per champion, keyed by the flattened page. The
-- keystone fixes the primary tree and the sub runes fix the secondary tree, so
-- both style ids are functionally dependent on the page CSVs; they are stored
-- alongside the page because the LCU rune-page API requires primaryStyleId and
-- subStyleId when creating a page.
CREATE TABLE IF NOT EXISTS rune_page_stat (
    patch              TEXT   NOT NULL,
    queue_id           INT    NOT NULL,
    champion_id        INT    NOT NULL,
    perk_primary_csv   TEXT   NOT NULL,
    perk_sub_csv       TEXT   NOT NULL,
    perk_shard_csv     TEXT   NOT NULL,
    games              BIGINT NOT NULL,
    wins               BIGINT NOT NULL,
    computed_at        BIGINT NOT NULL,
    perk_primary_style INT    NOT NULL DEFAULT 0,
    perk_sub_style     INT    NOT NULL DEFAULT 0,
    PRIMARY KEY (patch, queue_id, champion_id, perk_primary_csv, perk_sub_csv, perk_shard_csv)
);
