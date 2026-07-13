use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CollectError {
    #[error("riot api error: {0}")]
    Riot(#[from] rift::Error),

    #[error("store error: {0}")]
    Store(#[from] abyss_store::StoreError),

    #[error("core error: {0}")]
    Core(#[from] abyss_core::Error),

    #[error("match body is not valid utf-8: {0}")]
    BodyEncoding(#[from] std::string::FromUtf8Error),

    #[error("invalid match {match_id}: {reason}")]
    MatchInvalid {
        match_id: String,
        reason: &'static str,
    },
}
