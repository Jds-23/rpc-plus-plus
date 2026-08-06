use std::time::Duration;

pub trait Observer: Send + Sync + 'static {
    fn record_success(&self, upstream: &str, duration: Duration);
    fn record_failure(&self, upstream: &str, duration: Duration, http_status: Option<u16>);
}

pub struct NoopObserver {}

impl NoopObserver {
    pub fn new() -> Self {
        NoopObserver {}
    }
}

impl Observer for NoopObserver {
    fn record_failure(&self, upstream: &str, duration: Duration, http_status: Option<u16>) {
        let _upstream = upstream;
        let _duration = duration;
        let _http_status = http_status;
    }
    fn record_success(&self, upstream: &str, duration: Duration) {
        let _upstream = upstream;
        let _duration = duration;
    }
}
