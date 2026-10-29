use std::time::Duration;

use anyhow::{Context, Result};
use axum::http::{HeaderValue, Method, header};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::config::{CorsSettings, WILDCARD_ORIGIN};

const MAX_AGE: Duration = Duration::from_secs(3600);

pub(crate) fn layer(settings: &CorsSettings) -> Result<Option<CorsLayer>> {
    let origins = &settings.allowed_origins;
    if origins.is_empty() {
        return Ok(None);
    }

    let allow_origin = if origins.iter().any(|origin| origin == WILDCARD_ORIGIN) {
        AllowOrigin::any()
    } else {
        let origins = origins
            .iter()
            .map(|origin| {
                HeaderValue::from_str(origin)
                    .with_context(|| format!("invalid cors origin {origin:?}"))
            })
            .collect::<Result<Vec<_>>>()?;
        AllowOrigin::list(origins)
    };

    Ok(Some(
        CorsLayer::new()
            .allow_origin(allow_origin)
            .allow_methods([Method::POST])
            .allow_headers([header::CONTENT_TYPE])
            .max_age(MAX_AGE),
    ))
}
