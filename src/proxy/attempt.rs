use axum::body::Bytes;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::{
    observer::Observer,
    proxy::reply::Reply,
    upstream::{
        Upstream, UpstreamId,
        call::{CallError, CallOutcome, CallRecord},
    },
};

struct Attempt<'a> {
    observer: &'a dyn Observer,
    upstream: &'a UpstreamId,
    started: Instant,
    armed: bool,
}

impl<'a> Attempt<'a> {
    fn start(observer: &'a dyn Observer, upstream: &'a UpstreamId) -> Self {
        Attempt {
            observer,
            upstream,
            started: Instant::now(),
            armed: true,
        }
    }

    fn finish(mut self, record: CallRecord<'_>) {
        self.armed = false;
        self.observer.record(self.upstream, record);
    }
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.observer
                .record_abandoned(self.upstream, self.started.elapsed());
        }
    }
}

pub(super) async fn try_once(
    observer: &dyn Observer,
    upstream: &Upstream,
    body: &Bytes,
    attempt: u64,
    hedge: bool,
) -> Result<Reply, CallError> {
    let id = upstream.id();
    info!(
        event = "attempt_started",
        attempt,
        upstream = %id,
        hedge,
    );
    let in_flight = Attempt::start(observer, id);
    let call = upstream.call(body).await;
    in_flight.finish(call.record());
    let duration = call.duration.as_millis() as u64;
    match call.result {
        Ok(CallOutcome {
            http_status,
            response_body,
        }) => {
            info!(
                event = "attempt_succeeded",
                attempt,
                upstream = %id,
                duration_ms = duration,
                http_status = http_status.as_u16(),
                response_bytes = response_body.len(),
            );
            Ok(Reply {
                http_status,
                body: response_body,
            })
        }
        Err(failure) => {
            match &failure {
                CallError::Unreachable { error } => warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = duration,
                    error = %error,
                ),
                CallError::ReadFailed { http_status, error } => warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = duration,
                    http_status = http_status.as_u16(),
                    error = %error,
                ),
                CallError::RpcError {
                    http_status,
                    code,
                    retryable,
                } => warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = duration,
                    http_status = http_status.as_u16(),
                    rpc_error_code = code,
                    retryable,
                    error = "upstream returned a json-rpc error",
                ),
                CallError::ErrorStatus { http_status } => warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = duration,
                    http_status = http_status.as_u16(),
                    error = "upstream returned error status",
                ),
            }
            Err(failure)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::StatusCode;

    use super::*;
    use crate::observer::MetricsObserver;

    fn observer() -> (MetricsObserver, UpstreamId) {
        let id = UpstreamId::new("one");
        (MetricsObserver::new([id.clone()]), id)
    }

    #[test]
    fn a_finished_attempt_records_once() {
        let (observer, id) = observer();

        Attempt::start(&observer, &id).finish(CallRecord {
            outcome: Ok(StatusCode::OK),
            duration: Duration::from_millis(20),
        });

        let snapshot = observer.snapshot(&id).unwrap();
        assert_eq!(snapshot.success, 1);
        assert_eq!(snapshot.abandoned, 0, "finish disarms the drop");
        assert_eq!(snapshot.duration_micros_total, 20_000);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_attempt_records_the_time_it_ran() {
        let (observer, id) = observer();

        let deadline = Duration::from_millis(150);
        let dropped = tokio::time::timeout(deadline, async {
            let _attempt = Attempt::start(&observer, &id);
            std::future::pending::<()>().await
        })
        .await;

        assert!(dropped.is_err());
        let snapshot = observer.snapshot(&id).unwrap();
        assert_eq!(snapshot.abandoned, 1);
        assert_eq!(snapshot.success, 0);
        assert_eq!(snapshot.duration_micros_total, 150_000);
    }
}
