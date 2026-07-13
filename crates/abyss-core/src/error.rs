use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid platform: {0}")]
    InvalidPlatform(String),

    #[error("invalid patch: {0}")]
    InvalidPatch(String),

    #[error("invalid seed target, expected name#tag@platform: {0}")]
    InvalidSeedTarget(String),
}
