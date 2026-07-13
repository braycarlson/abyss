use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::header::{AUTHORIZATION, HeaderValue};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

const RATE_WINDOW_MS: i64 = 60_000;
const RATE_CLIENTS_MAX: usize = 10_000;
const REQUEST_TIMEOUT_S: u64 = 30;
const FORWARDED_CHARS_MAX: usize = 64;
const HEALTH_PATH: &str = "/health";
const ABOUT_PATH: &str = "/about";
const DRAGON_PATH_PREFIX: &str = "/dragon/";
const RIOT_PROXY_PATH_PREFIX: &str = "/players/by-riot-id/";

pub struct ApiConfig {
    pub bearer_token: Option<String>,
    pub concurrent_requests_max: usize,
    pub cors_origin: String,
    pub dragon_cache_bytes_max: u64,
    pub dragon_dir: PathBuf,
    pub dragon_rate_limit_per_minute: u32,
    pub rate_limit_per_minute: u32,
    pub riot_rate_limit_per_minute: u32,
    pub trusted_proxy: bool,
}

pub(crate) struct RateLimiter {
    clients: Mutex<HashMap<IpAddr, RateWindow>>,
    limit_per_minute: u32,
}

struct RateWindow {
    window_start_ms: i64,
    count: u32,
}

impl RateLimiter {
    pub(crate) fn new(limit_per_minute: u32) -> RateLimiter {
        assert!(limit_per_minute >= 1, "rate limit must be positive");

        RateLimiter {
            clients: Mutex::new(HashMap::new()),
            limit_per_minute,
        }
    }

    fn admit(&self, ip: IpAddr, now_ms: i64) -> bool {
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if !clients.contains_key(&ip) && clients.len() >= RATE_CLIENTS_MAX {
            clients
                .retain(|_, window| now_ms.saturating_sub(window.window_start_ms) < RATE_WINDOW_MS);

            if clients.len() >= RATE_CLIENTS_MAX {
                return false;
            }
        }

        let window = clients.entry(ip).or_insert(RateWindow {
            window_start_ms: now_ms,
            count: 0,
        });

        if now_ms.saturating_sub(window.window_start_ms) >= RATE_WINDOW_MS {
            window.window_start_ms = now_ms;
            window.count = 0;
        }

        window.count = window.count.saturating_add(1);

        window.count <= self.limit_per_minute
    }
}

pub(crate) async fn guard(
    State(state): State<crate::AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let origin = state.config.cors_origin.clone();

    if request.method() == Method::OPTIONS {
        let response = StatusCode::NO_CONTENT.into_response();

        return cors_apply(response, &origin);
    }

    let ip = client_ip(&request, addr.ip(), state.config.trusted_proxy);

    if let Some((status, message)) = admission_denial(&state, &request, ip) {
        let response = (status, message).into_response();

        return cors_apply(response, &origin);
    }

    let Ok(_permit) = state.requests.try_acquire() else {
        let response = (StatusCode::SERVICE_UNAVAILABLE, "server busy").into_response();

        return cors_apply(response, &origin);
    };

    let deadline = Duration::from_secs(REQUEST_TIMEOUT_S);

    let Ok(response) = tokio::time::timeout(deadline, next.run(request)).await else {
        let response = (StatusCode::GATEWAY_TIMEOUT, "request timed out").into_response();

        return cors_apply(response, &origin);
    };

    cors_apply(response, &origin)
}

fn admission_denial(
    state: &crate::AppState,
    request: &Request,
    ip: IpAddr,
) -> Option<(StatusCode, &'static str)> {
    let path = request.uri().path();

    if path == HEALTH_PATH || path == ABOUT_PATH {
        return None;
    }

    let now_ms = abyss_core::now_ms();

    if path.starts_with(DRAGON_PATH_PREFIX) {
        if !state.dragon_rate_limiter.admit(ip, now_ms) {
            return Some((StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded"));
        }

        return None;
    }

    if request.method() == Method::POST && state.config.bearer_token.is_none() {
        return Some((
            StatusCode::FORBIDDEN,
            "mutating requests are disabled without an api token",
        ));
    }

    if let Some(token) = &state.config.bearer_token
        && !authorized(request, token)
    {
        return Some((StatusCode::UNAUTHORIZED, "missing or invalid bearer token"));
    }

    if path.starts_with(RIOT_PROXY_PATH_PREFIX) && !state.riot_rate_limiter.admit(ip, now_ms) {
        return Some((StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded"));
    }

    if !state.rate_limiter.admit(ip, now_ms) {
        return Some((StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded"));
    }

    None
}

fn client_ip(request: &Request, peer: IpAddr, trusted_proxy: bool) -> IpAddr {
    if !trusted_proxy {
        return peer;
    }

    let Some(header) = request.headers().get("x-forwarded-for") else {
        return peer;
    };

    let Ok(value) = header.to_str() else {
        return peer;
    };

    let candidate = value.rsplit(',').next().unwrap_or("").trim();

    if candidate.is_empty() || candidate.len() > FORWARDED_CHARS_MAX {
        return peer;
    }

    candidate.parse().unwrap_or(peer)
}

fn authorized(request: &Request, token: &str) -> bool {
    let Some(header) = request.headers().get(AUTHORIZATION) else {
        return false;
    };

    let Ok(value) = header.to_str() else {
        return false;
    };

    let Some(presented) = value.strip_prefix("Bearer ") else {
        return false;
    };

    constant_time_eq(presented.as_bytes(), token.as_bytes())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut difference: u8 = 0;

    for index in 0..left.len() {
        difference |= left[index] ^ right[index];
    }

    difference == 0
}

fn cors_apply(mut response: Response, origin: &str) -> Response {
    let headers = response.headers_mut();

    if let Ok(value) = HeaderValue::from_str(origin) {
        headers.insert("access-control-allow-origin", value);
    }

    headers.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );

    headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("authorization, content-type"),
    );

    if origin != "*" {
        headers.insert("vary", HeaderValue::from_static("origin"));
    }

    response
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use axum::body::Body;
    use axum::extract::Request;

    use super::{RATE_WINDOW_MS, RateLimiter, client_ip, constant_time_eq};

    fn request_with_forwarded(value: &str) -> Request {
        Request::builder()
            .header("x-forwarded-for", value)
            .body(Body::empty())
            .expect("request builds")
    }

    #[test]
    fn rate_limiter_enforces_window_budget() {
        let limiter = RateLimiter::new(2);
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        assert!(limiter.admit(ip, 0));
        assert!(limiter.admit(ip, 1));
        assert!(!limiter.admit(ip, 2));

        assert!(limiter.admit(ip, RATE_WINDOW_MS));
    }

    #[test]
    fn constant_time_eq_compares_exactly() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret-longer"));
    }

    #[test]
    fn client_ip_ignores_forwarded_without_trust() {
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let request = request_with_forwarded("203.0.113.7");

        assert_eq!(client_ip(&request, peer, false), peer);
    }

    #[test]
    fn client_ip_takes_rightmost_forwarded_hop_when_trusted() {
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let request = request_with_forwarded("198.51.100.9, 203.0.113.7");
        let expected = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));

        assert_eq!(client_ip(&request, peer, true), expected);
    }

    #[test]
    fn client_ip_parses_forwarded_ipv6() {
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let request = request_with_forwarded("2001:db8::1");
        let expected = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));

        assert_eq!(client_ip(&request, peer, true), expected);
    }

    #[test]
    fn client_ip_falls_back_on_garbage_forwarded() {
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        assert_eq!(
            client_ip(&request_with_forwarded("not-an-ip"), peer, true),
            peer
        );
        assert_eq!(client_ip(&request_with_forwarded(""), peer, true), peer);
        assert_eq!(
            client_ip(&request_with_forwarded(&"1".repeat(200)), peer, true),
            peer
        );
    }
}
