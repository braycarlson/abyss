use rift::routes::{PlatformRoute, RegionalRoute};
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum RegionalGroup {
    Americas,
    Asia,
    Europe,
    Sea,
}

impl RegionalGroup {
    pub const ALL: [RegionalGroup; 4] = [
        RegionalGroup::Americas,
        RegionalGroup::Asia,
        RegionalGroup::Europe,
        RegionalGroup::Sea,
    ];

    #[must_use]
    pub fn regional_route(self) -> RegionalRoute {
        match self {
            RegionalGroup::Americas => RegionalRoute::Americas,
            RegionalGroup::Asia => RegionalRoute::Asia,
            RegionalGroup::Europe => RegionalRoute::Europe,
            RegionalGroup::Sea => RegionalRoute::Sea,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RegionalGroup::Americas => "AMERICAS",
            RegionalGroup::Asia => "ASIA",
            RegionalGroup::Europe => "EUROPE",
            RegionalGroup::Sea => "SEA",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Platform {
    Br1,
    Eun1,
    Euw1,
    Jp1,
    Kr,
    La1,
    La2,
    Me1,
    Na1,
    Oc1,
    Ph2,
    Ru,
    Sg2,
    Th2,
    Tr1,
    Tw2,
    Vn2,
}

impl Platform {
    pub const ALL: [Platform; 17] = [
        Platform::Br1,
        Platform::Eun1,
        Platform::Euw1,
        Platform::Jp1,
        Platform::Kr,
        Platform::La1,
        Platform::La2,
        Platform::Me1,
        Platform::Na1,
        Platform::Oc1,
        Platform::Ph2,
        Platform::Ru,
        Platform::Sg2,
        Platform::Th2,
        Platform::Tr1,
        Platform::Tw2,
        Platform::Vn2,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Platform::Br1 => "BR1",
            Platform::Eun1 => "EUN1",
            Platform::Euw1 => "EUW1",
            Platform::Jp1 => "JP1",
            Platform::Kr => "KR",
            Platform::La1 => "LA1",
            Platform::La2 => "LA2",
            Platform::Me1 => "ME1",
            Platform::Na1 => "NA1",
            Platform::Oc1 => "OC1",
            Platform::Ph2 => "PH2",
            Platform::Ru => "RU",
            Platform::Sg2 => "SG2",
            Platform::Th2 => "TH2",
            Platform::Tr1 => "TR1",
            Platform::Tw2 => "TW2",
            Platform::Vn2 => "VN2",
        }
    }

    pub fn parse(text: &str) -> Result<Platform, Error> {
        let upper = text.to_ascii_uppercase();

        let platform = match upper.as_str() {
            "BR1" => Platform::Br1,
            "EUN1" => Platform::Eun1,
            "EUW1" => Platform::Euw1,
            "JP1" => Platform::Jp1,
            "KR" => Platform::Kr,
            "LA1" => Platform::La1,
            "LA2" => Platform::La2,
            "ME1" => Platform::Me1,
            "NA1" => Platform::Na1,
            "OC1" => Platform::Oc1,
            "PH2" => Platform::Ph2,
            "RU" => Platform::Ru,
            "SG2" => Platform::Sg2,
            "TH2" => Platform::Th2,
            "TR1" => Platform::Tr1,
            "TW2" => Platform::Tw2,
            "VN2" => Platform::Vn2,
            _ => return Err(Error::InvalidPlatform(text.to_string())),
        };

        Ok(platform)
    }

    #[must_use]
    pub fn from_match_id(match_id: &str) -> Option<Platform> {
        let (prefix, _rest) = match_id.split_once('_')?;

        Platform::parse(prefix).ok()
    }

    #[must_use]
    pub fn platform_route(self) -> PlatformRoute {
        match self {
            Platform::Br1 => PlatformRoute::Br1,
            Platform::Eun1 => PlatformRoute::Eun1,
            Platform::Euw1 => PlatformRoute::Euw1,
            Platform::Jp1 => PlatformRoute::Jp1,
            Platform::Kr => PlatformRoute::Kr,
            Platform::La1 => PlatformRoute::La1,
            Platform::La2 => PlatformRoute::La2,
            Platform::Me1 => PlatformRoute::Me1,
            Platform::Na1 => PlatformRoute::Na1,
            Platform::Oc1 => PlatformRoute::Oc1,
            Platform::Ph2 => PlatformRoute::Ph2,
            Platform::Ru => PlatformRoute::Ru,
            Platform::Sg2 => PlatformRoute::Sg2,
            Platform::Th2 => PlatformRoute::Th2,
            Platform::Tr1 => PlatformRoute::Tr1,
            Platform::Tw2 => PlatformRoute::Tw2,
            Platform::Vn2 => PlatformRoute::Vn2,
        }
    }

    #[must_use]
    pub fn regional_group(self) -> RegionalGroup {
        match self {
            Platform::Br1 | Platform::La1 | Platform::La2 | Platform::Na1 => {
                RegionalGroup::Americas
            }
            Platform::Jp1 | Platform::Kr => RegionalGroup::Asia,
            Platform::Eun1 | Platform::Euw1 | Platform::Me1 | Platform::Ru | Platform::Tr1 => {
                RegionalGroup::Europe
            }
            Platform::Oc1
            | Platform::Ph2
            | Platform::Sg2
            | Platform::Th2
            | Platform::Tw2
            | Platform::Vn2 => RegionalGroup::Sea,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Platform, RegionalGroup};

    #[test]
    fn parse_is_case_insensitive() {
        assert_eq!(Platform::parse("na1").unwrap(), Platform::Na1);
        assert_eq!(Platform::parse("KR").unwrap(), Platform::Kr);
        assert!(Platform::parse("zz9").is_err());
    }

    #[test]
    fn from_match_id_reads_prefix() {
        assert_eq!(
            Platform::from_match_id("NA1_1234567890"),
            Some(Platform::Na1)
        );
        assert_eq!(Platform::from_match_id("KR_98765"), Some(Platform::Kr));
        assert_eq!(Platform::from_match_id("garbage"), None);
    }

    #[test]
    fn regional_groups_route_correctly() {
        assert_eq!(Platform::Kr.regional_group(), RegionalGroup::Asia);
        assert_eq!(Platform::Euw1.regional_group(), RegionalGroup::Europe);
        assert_eq!(Platform::Na1.regional_group(), RegionalGroup::Americas);
        assert_eq!(Platform::Oc1.regional_group(), RegionalGroup::Sea);
    }

    #[test]
    fn every_platform_round_trips_through_parse() {
        for platform in Platform::ALL {
            assert_eq!(Platform::parse(platform.as_str()).unwrap(), platform);
        }
    }
}
