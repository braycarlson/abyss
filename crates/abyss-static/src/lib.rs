use std::collections::{HashMap, HashSet};

use abyss_core::Patch;
use dragon::models::ItemsFile;
use dragon::{DragonApi, DragonApiConfig};
use thiserror::Error;

const LOCALE: &str = "en_US";
const CORE_GOLD_MIN: u32 = 2_000;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StaticError {
    #[error("dragon error: {0}")]
    Dragon(#[from] dragon::Error),

    #[error("invalid version from data dragon: {0}")]
    InvalidVersion(String),
}

pub struct Names {
    patch: String,
    version: String,
    champions: HashMap<i32, String>,
    items: HashMap<i32, String>,
    item_core: HashSet<i32>,
    item_boots: HashSet<i32>,
    runes: HashMap<i32, String>,
    spells: HashMap<i32, String>,
}

impl Names {
    pub async fn load() -> Result<Names, StaticError> {
        let api = DragonApi::new(DragonApiConfig::new())?;
        let version = api.version_latest_fetch().await?;

        let champions = load_champions(&api, &version).await?;
        let items_file = api.items_fetch(&version, LOCALE).await?;
        let runes = load_runes(&api, &version).await?;
        let spells = load_spells(&api, &version).await?;

        let (items, item_core, item_boots) = classify_items(&items_file);

        let patch = Patch::from_game_version(&version)
            .ok_or_else(|| StaticError::InvalidVersion(version.clone()))?
            .label();

        Ok(Names {
            patch,
            version,
            champions,
            items,
            item_core,
            item_boots,
            runes,
            spells,
        })
    }

    #[must_use]
    pub fn patch(&self) -> &str {
        &self.patch
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    #[must_use]
    pub fn champion(&self, id: i32) -> Option<&str> {
        self.champions.get(&id).map(String::as_str)
    }

    #[must_use]
    pub fn item(&self, id: i32) -> Option<&str> {
        self.items.get(&id).map(String::as_str)
    }

    #[must_use]
    pub fn rune(&self, id: i32) -> Option<&str> {
        self.runes.get(&id).map(String::as_str)
    }

    #[must_use]
    pub fn spell(&self, id: i32) -> Option<&str> {
        self.spells.get(&id).map(String::as_str)
    }

    #[must_use]
    pub fn item_is_core(&self, id: i32) -> bool {
        self.item_core.contains(&id)
    }

    #[must_use]
    pub fn item_is_boots(&self, id: i32) -> bool {
        self.item_boots.contains(&id)
    }
}

pub async fn current_patch() -> Result<Patch, StaticError> {
    let api = DragonApi::new(DragonApiConfig::new())?;
    let version = api.version_latest_fetch().await?;

    Patch::from_game_version(&version).ok_or(StaticError::InvalidVersion(version))
}

fn classify_items(file: &ItemsFile) -> (HashMap<i32, String>, HashSet<i32>, HashSet<i32>) {
    let mut names: HashMap<i32, String> = HashMap::with_capacity(file.data.len());
    let mut core: HashSet<i32> = HashSet::new();
    let mut boots: HashSet<i32> = HashSet::new();

    for (key, item) in &file.data {
        let Ok(id) = key.parse::<i32>() else {
            continue;
        };

        names.insert(id, item.name.to_string());

        let is_boots = item.tags.iter().any(|tag| tag == "Boots");

        if is_boots {
            boots.insert(id);
        }

        let is_special = item
            .tags
            .iter()
            .any(|tag| tag == "Consumable" || tag == "Trinket");

        let is_core = item.builds_into.is_none()
            && item.gold.purchasable
            && item.gold.total >= CORE_GOLD_MIN
            && !is_boots
            && !is_special;

        if is_core {
            core.insert(id);
        }
    }

    (names, core, boots)
}

async fn load_champions(
    api: &DragonApi,
    version: &str,
) -> Result<HashMap<i32, String>, StaticError> {
    let file = api.champions_fetch(version, LOCALE).await?;
    let mut names: HashMap<i32, String> = HashMap::with_capacity(file.data.len());

    for champion in file.data.values() {
        if let Ok(id) = champion.key.parse::<i32>() {
            names.insert(id, champion.name.to_string());
        }
    }

    Ok(names)
}

async fn load_spells(api: &DragonApi, version: &str) -> Result<HashMap<i32, String>, StaticError> {
    let file = api.summoner_spells_fetch(version, LOCALE).await?;
    let mut names: HashMap<i32, String> = HashMap::with_capacity(file.data.len());

    for spell in file.data.values() {
        if let Ok(id) = spell.key.parse::<i32>() {
            names.insert(id, spell.name.to_string());
        }
    }

    Ok(names)
}

async fn load_runes(api: &DragonApi, version: &str) -> Result<HashMap<i32, String>, StaticError> {
    let trees = api.runes_fetch(version, LOCALE).await?;
    let mut names: HashMap<i32, String> = HashMap::new();

    for tree in &trees {
        if let Ok(id) = i32::try_from(tree.id) {
            names.insert(id, tree.name.to_string());
        }

        for slot in &tree.slots {
            for rune in &slot.runes {
                if let Ok(id) = i32::try_from(rune.id) {
                    names.insert(id, rune.name.to_string());
                }
            }
        }
    }

    Ok(names)
}
