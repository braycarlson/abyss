use std::path::{Path as FilePath, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::AppState;

pub(crate) const DRAGON_FETCHES_MAX: usize = 8;

const CACHE_SCAN_ENTRIES_MAX: u32 = 1_000_000;

const DDRAGON_BASE: &str = "https://ddragon.leagueoflegends.com";
const CDRAGON_RAW_BASE: &str = "https://raw.communitydragon.org";
const CDRAGON_CDN_BASE: &str = "https://cdn.communitydragon.org";
const CDRAGON_PATH_PREFIX: &str = "latest/plugins/rcp-be-lol-game-data/";
const VERSIONS_PATH: &str = "api/versions.json";
const PATH_CHARS_MAX: usize = 300;
const PATH_SEGMENTS_MAX: usize = 16;
const BODY_BYTES_MAX: usize = 16 * 1024 * 1024;
const VOLATILE_TTL: Duration = Duration::from_hours(6);
const CACHE_CONTROL_IMMUTABLE: &str = "public, max-age=31536000, immutable";
const CACHE_CONTROL_VOLATILE: &str = "public, max-age=21600";

pub(crate) async fn ddragon(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    let allowed = path == VERSIONS_PATH || path.starts_with("cdn/");

    let Some(relative) = sanitized(&path).filter(|_| allowed) else {
        return (StatusCode::NOT_FOUND, "unknown asset path").into_response();
    };

    let url = format!("{DDRAGON_BASE}/{path}");
    let file = PathBuf::from("ddragon").join(relative);

    serve_asset(&state, &url, &file, volatile(&path)).await
}

pub(crate) async fn cdragon(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    let allowed = path.starts_with(CDRAGON_PATH_PREFIX);

    let Some(relative) = sanitized(&path).filter(|_| allowed) else {
        return (StatusCode::NOT_FOUND, "unknown asset path").into_response();
    };

    let url = format!("{CDRAGON_RAW_BASE}/{path}");
    let file = PathBuf::from("cdragon").join(relative);

    serve_asset(&state, &url, &file, volatile(&path)).await
}

pub(crate) async fn champion_square(
    State(state): State<AppState>,
    Path(champion_id): Path<i32>,
) -> Response {
    if champion_id <= 0 {
        return (StatusCode::NOT_FOUND, "unknown champion id").into_response();
    }

    let url = format!("{CDRAGON_CDN_BASE}/latest/champion/{champion_id}/square");
    let file = PathBuf::from("csquare").join(format!("{champion_id}.png"));

    serve_asset(&state, &url, &file, true).await
}

fn volatile(path: &str) -> bool {
    path == VERSIONS_PATH || path.starts_with("latest/") || path.contains("/latest/")
}

fn sanitized(path: &str) -> Option<PathBuf> {
    if path.is_empty() || path.len() > PATH_CHARS_MAX {
        return None;
    }

    let segments: Vec<&str> = path.split('/').collect();

    if segments.len() > PATH_SEGMENTS_MAX {
        return None;
    }

    let mut relative = PathBuf::new();

    for segment in segments {
        let name_safe = !segment.is_empty() && segment != "." && segment != "..";

        let chars_safe = segment
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '@'));

        if !name_safe || !chars_safe {
            return None;
        }

        relative.push(segment);
    }

    Some(relative)
}

async fn serve_asset(state: &AppState, url: &str, relative: &FilePath, volatile: bool) -> Response {
    let file = state.config.dragon_dir.join(relative);

    if let Some(bytes) = cache_read(&file, volatile).await {
        return asset_response(bytes, relative, volatile);
    }

    // Bound concurrent upstream fetches so a burst of cache misses cannot
    // amplify into unbounded outbound traffic against the asset CDNs.
    let Ok(_permit) = state.dragon_fetches.try_acquire() else {
        if let Ok(bytes) = tokio::fs::read(&file).await {
            return asset_response(bytes, relative, volatile);
        }

        return (StatusCode::SERVICE_UNAVAILABLE, "upstream fetch busy").into_response();
    };

    match upstream_fetch(state, url).await {
        Ok(bytes) => {
            cache_write(state, &file, &bytes).await;

            asset_response(bytes, relative, volatile)
        }
        Err(error) => {
            if let Ok(bytes) = tokio::fs::read(&file).await {
                return asset_response(bytes, relative, volatile);
            }

            tracing::warn!(url, error = %error, "dragon upstream fetch failed");

            (StatusCode::BAD_GATEWAY, "upstream fetch failed").into_response()
        }
    }
}

pub(crate) fn cache_bytes_scan(root: &FilePath) -> u64 {
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    let mut total: u64 = 0;
    let mut entries: u32 = 0;

    while let Some(directory) = stack.pop() {
        let Ok(reader) = std::fs::read_dir(&directory) else {
            continue;
        };

        for entry in reader.flatten() {
            entries += 1;

            if entries > CACHE_SCAN_ENTRIES_MAX {
                tracing::warn!(path = %root.display(), "dragon cache scan truncated");

                return total;
            }

            let Ok(metadata) = entry.metadata() else {
                continue;
            };

            if metadata.is_dir() {
                stack.push(entry.path());
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }

    total
}

async fn cache_read(file: &FilePath, volatile: bool) -> Option<Vec<u8>> {
    let metadata = tokio::fs::metadata(file).await.ok()?;

    if volatile {
        let modified = metadata.modified().ok()?;

        let age = SystemTime::now()
            .duration_since(modified)
            .unwrap_or_default();

        if age > VOLATILE_TTL {
            return None;
        }
    }

    tokio::fs::read(file).await.ok()
}

async fn upstream_fetch(state: &AppState, url: &str) -> Result<Vec<u8>, String> {
    let response = state
        .http
        .get(url)
        .send()
        .await
        .map_err(|error| error.to_string())?;

    let status = response.status();

    if !status.is_success() {
        return Err(status.to_string());
    }

    if let Some(length) = response.content_length()
        && length > BODY_BYTES_MAX as u64
    {
        return Err(format!("asset exceeds {BODY_BYTES_MAX} byte limit"));
    }

    let bytes = response.bytes().await.map_err(|error| error.to_string())?;

    if bytes.len() > BODY_BYTES_MAX {
        return Err(format!("asset exceeds {BODY_BYTES_MAX} byte limit"));
    }

    Ok(bytes.to_vec())
}

async fn cache_write(state: &AppState, file: &FilePath, bytes: &[u8]) {
    let written = bytes.len() as u64;

    let existing = match tokio::fs::metadata(file).await {
        Ok(metadata) => metadata.len(),
        Err(_) => 0,
    };

    // The counter is a bound, not an inventory: overwrites subtract the old
    // size so volatile assets do not creep the total toward the cap.
    let stored = state.dragon_cache_bytes.load(Ordering::Relaxed);
    let projected = stored.saturating_sub(existing).saturating_add(written);

    if projected > state.config.dragon_cache_bytes_max {
        tracing::warn!(
            stored,
            written,
            cap = state.config.dragon_cache_bytes_max,
            "dragon cache full; skipping write"
        );

        return;
    }

    if let Some(parent) = file.parent()
        && let Err(error) = tokio::fs::create_dir_all(parent).await
    {
        tracing::warn!(path = %parent.display(), error = %error, "dragon cache dir failed");

        return;
    }

    let temp = file.with_extension("part");

    if let Err(error) = tokio::fs::write(&temp, bytes).await {
        tracing::warn!(path = %temp.display(), error = %error, "dragon cache write failed");

        return;
    }

    if let Err(error) = tokio::fs::rename(&temp, file).await {
        tracing::warn!(path = %file.display(), error = %error, "dragon cache rename failed");

        return;
    }

    state
        .dragon_cache_bytes
        .fetch_add(written, Ordering::Relaxed);

    if existing > 0 {
        state
            .dragon_cache_bytes
            .fetch_sub(existing, Ordering::Relaxed);
    }
}

fn asset_response(bytes: Vec<u8>, relative: &FilePath, volatile: bool) -> Response {
    let cache_control = if volatile {
        CACHE_CONTROL_VOLATILE
    } else {
        CACHE_CONTROL_IMMUTABLE
    };

    let headers = [
        (header::CONTENT_TYPE, content_type_of(relative)),
        (header::CACHE_CONTROL, cache_control),
    ];

    (headers, bytes).into_response()
}

fn content_type_of(path: &FilePath) -> &'static str {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");

    match extension {
        "json" => "application/json",
        "jpg" | "jpeg" => "image/jpeg",
        "svg" => "image/svg+xml",
        "webp" => "image/webp",
        _ => "image/png",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{content_type_of, sanitized, volatile};

    #[test]
    fn sanitized_accepts_cdn_paths() {
        let relative = sanitized("cdn/16.13.1/img/item/3006.png").expect("valid path");

        assert_eq!(relative, PathBuf::from("cdn/16.13.1/img/item/3006.png"));
    }

    #[test]
    fn sanitized_rejects_traversal_and_odd_characters() {
        assert!(sanitized("cdn/../secrets").is_none());
        assert!(sanitized("cdn//double").is_none());
        assert!(sanitized("cdn/a b").is_none());
        assert!(sanitized("cdn/a\\b").is_none());
        assert!(sanitized("").is_none());
    }

    #[test]
    fn volatile_tracks_unpinned_paths() {
        let unpinned =
            "latest/plugins/rcp-be-lol-game-data/global/default/v1/champion-summary.json";

        assert!(volatile("api/versions.json"));
        assert!(volatile(unpinned));
        assert!(!volatile("cdn/16.13.1/data/en_US/item.json"));
        assert!(!volatile("cdn/img/perk-images/Styles/7200_Domination.png"));
    }

    #[test]
    fn content_type_follows_extension() {
        assert_eq!(
            content_type_of(&PathBuf::from("a/item.json")),
            "application/json"
        );
        assert_eq!(content_type_of(&PathBuf::from("a/tile.jpg")), "image/jpeg");
        assert_eq!(
            content_type_of(&PathBuf::from("csquare/22.png")),
            "image/png"
        );
    }
}
