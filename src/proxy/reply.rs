use axum::{
    body::Bytes,
    response::{IntoResponse, Response},
};
use reqwest::{StatusCode, header};

use crate::jsonrpc::rpc_error_body;

/// What a request answered, in a form every caller can hold. A `Response` body
/// reads once; `Bytes::clone` is a refcount bump.
#[derive(Debug, Clone)]
pub(crate) struct Reply {
    pub status: StatusCode,
    pub body: Bytes,
}

impl Reply {
    pub(crate) fn rpc_error(code: i64, msg: &str) -> Self {
        Self {
            status: StatusCode::OK,
            body: rpc_error_body(code, msg),
        }
    }
}

impl IntoResponse for Reply {
    fn into_response(self) -> Response {
        (
            self.status,
            [(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())],
            self.body,
        )
            .into_response()
    }
}
