use abyss_core::now_ms;
use abyss_store::{Store, StoreError};

pub async fn rebuild(store: &Store, patch: Option<&str>) -> Result<(), StoreError> {
    if let Some(patch) = patch {
        assert!(!patch.trim().is_empty(), "patch filter must not be blank");
    }

    let now = now_ms();

    store.rebuild_stats(now, patch).await
}
