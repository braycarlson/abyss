use std::time::{SystemTime, UNIX_EPOCH};

#[must_use]
pub fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}
