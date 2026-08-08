use crate::upstream::{CallRecord, UpstreamId};

pub trait Observer: Send + Sync + 'static {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>);
}

#[derive(Default)]
pub struct NoopObserver {}

impl NoopObserver {
    pub fn new() -> Self {
        NoopObserver::default()
    }
}

impl Observer for NoopObserver {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>) {
        let _upstream = upstream;
        let _record = record;
    }
}
