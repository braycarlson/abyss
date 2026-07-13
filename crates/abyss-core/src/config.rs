use crate::error::Error;
use crate::patch::Patch;
use crate::platform::Platform;

pub const WORKERS_PER_ROUTE_MAX: u32 = 64;
pub const IDS_PER_ACCOUNT_MAX: u32 = 1_000;
pub const RESEED_INTERVAL_HOURS_MAX: u32 = 168;
pub const AGGREGATE_INTERVAL_MINUTES_MAX: u32 = 1_440;
pub const AGGREGATE_MATCHES_MIN_MAX: u32 = 100_000;
pub const REVISIT_AFTER_HOURS_MAX: u32 = 720;
pub const REVISIT_BATCH_MAX: u32 = 100_000;
pub const RECOVER_INTERVAL_MINUTES_MAX: u32 = 1_440;
pub const CLAIM_TTL_MINUTES_MAX: u32 = 1_440;
pub const CLAIM_TTL_MINUTES_MIN: u32 = 5;

#[derive(Clone, Debug)]
pub struct CrawlConfig {
    pub regions: Vec<Platform>,
    pub focus_queues: Vec<u16>,
    pub patch_floor: Option<Patch>,
    pub since_unix: Option<i64>,
    pub workers_per_route: u32,
    pub ids_per_account: u32,
    pub fetch_timeline: bool,
    pub service_mode: bool,
    pub reseed_interval_hours: u32,
    pub aggregate_interval_minutes: u32,
    pub aggregate_matches_min: u32,
    pub revisit_after_hours: u32,
    pub revisit_batch: u32,
    pub recover_interval_minutes: u32,
    pub claim_ttl_minutes: u32,
}

impl CrawlConfig {
    #[must_use]
    pub fn priority_of(&self, platform: Platform) -> i32 {
        let count = self.regions.len();

        debug_assert!(
            count <= Platform::ALL.len(),
            "more regions than platforms exist"
        );

        let index = self
            .regions
            .iter()
            .position(|&candidate| candidate == platform)
            .unwrap_or(count);

        debug_assert!(index <= count, "position must fall within the region list");

        i32::try_from(index).unwrap_or(i32::MAX)
    }
}

impl Default for CrawlConfig {
    fn default() -> CrawlConfig {
        CrawlConfig {
            regions: vec![Platform::Kr, Platform::Euw1, Platform::Na1],
            focus_queues: vec![crate::QUEUE_ARAM],
            patch_floor: None,
            since_unix: None,
            workers_per_route: 4,
            ids_per_account: 50,
            fetch_timeline: false,
            service_mode: false,
            reseed_interval_hours: 24,
            aggregate_interval_minutes: 15,
            aggregate_matches_min: 50,
            revisit_after_hours: 24,
            revisit_batch: 10_000,
            recover_interval_minutes: 10,
            claim_ttl_minutes: 30,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SeedTarget {
    pub game_name: String,
    pub tag_line: String,
    pub platform: Platform,
}

impl SeedTarget {
    pub fn parse(text: &str) -> Result<SeedTarget, Error> {
        let (identity, platform_text) = text
            .rsplit_once('@')
            .ok_or_else(|| Error::InvalidSeedTarget(text.to_string()))?;

        let (game_name, tag_line) = identity
            .split_once('#')
            .ok_or_else(|| Error::InvalidSeedTarget(text.to_string()))?;

        if game_name.is_empty() || tag_line.is_empty() {
            return Err(Error::InvalidSeedTarget(text.to_string()));
        }

        let platform = Platform::parse(platform_text)?;

        Ok(SeedTarget {
            game_name: game_name.to_string(),
            tag_line: tag_line.to_string(),
            platform,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{CrawlConfig, SeedTarget};
    use crate::platform::Platform;

    #[test]
    fn priority_follows_region_order() {
        let config = CrawlConfig {
            regions: vec![Platform::Kr, Platform::Euw1, Platform::Na1],
            ..CrawlConfig::default()
        };

        assert_eq!(config.priority_of(Platform::Kr), 0);
        assert_eq!(config.priority_of(Platform::Euw1), 1);
        assert_eq!(config.priority_of(Platform::Na1), 2);
        assert_eq!(config.priority_of(Platform::Br1), 3);
    }

    #[test]
    fn seed_target_parses_name_tag_platform() {
        let target = SeedTarget::parse("Faker#KR1@KR").unwrap();

        assert_eq!(target.game_name, "Faker");
        assert_eq!(target.tag_line, "KR1");
        assert_eq!(target.platform, Platform::Kr);
    }

    #[test]
    fn seed_target_rejects_malformed() {
        assert!(SeedTarget::parse("no-at-or-hash").is_err());
        assert!(SeedTarget::parse("name@KR").is_err());
        assert!(SeedTarget::parse("#tag@KR").is_err());
    }
}
