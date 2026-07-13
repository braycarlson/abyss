use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Patch {
    pub major: u16,
    pub minor: u16,
}

impl Patch {
    #[must_use]
    pub fn new(major: u16, minor: u16) -> Patch {
        Patch { major, minor }
    }

    #[must_use]
    pub fn from_game_version(game_version: &str) -> Option<Patch> {
        let mut parts = game_version.split('.');

        let major = parts.next()?.parse::<u16>().ok()?;
        let minor = parts.next()?.parse::<u16>().ok()?;

        Some(Patch { major, minor })
    }

    pub fn parse(text: &str) -> Result<Patch, Error> {
        Patch::from_game_version(text).ok_or_else(|| Error::InvalidPatch(text.to_string()))
    }

    #[must_use]
    pub fn label(self) -> String {
        format!("{}.{}", self.major, self.minor)
    }
}

impl std::fmt::Display for Patch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.major, self.minor)
    }
}

#[cfg(test)]
mod tests {
    use super::Patch;

    #[test]
    fn from_game_version_takes_major_minor() {
        let patch = Patch::from_game_version("14.13.579.1234").unwrap();

        assert_eq!(patch, Patch::new(14, 13));
        assert_eq!(patch.label(), "14.13");
    }

    #[test]
    fn from_game_version_rejects_garbage() {
        assert!(Patch::from_game_version("not-a-version").is_none());
        assert!(Patch::from_game_version("14").is_none());
    }

    #[test]
    fn ordering_is_numeric_not_lexicographic() {
        assert!(Patch::new(14, 9) < Patch::new(14, 13));
        assert!(Patch::new(15, 1) > Patch::new(14, 24));
    }
}
