use std::{collections::HashMap, sync::Arc};

use prometheus::{
    core::{Collector, Desc},
    proto::{Counter, LabelPair, Metric, MetricFamily, MetricType},
};

use crate::{
    observer::{MetricsObserver, StatsSnapshot},
    upstream::CallError,
};

const ATTEMPTS_NAME: &str = "rpc_attempts_total";
const ATTEMPTS_HELP: &str = "Total upstream call attempts by outcome";
const SUCCESS: &str = "success";

pub struct MetricsCollector {
    observer: Arc<MetricsObserver>,
    attempts_desc: Desc,
}

impl MetricsCollector {
    pub fn new(observer: Arc<MetricsObserver>) -> prometheus::Result<Self> {
        let attempts_desc = Desc::new(
            ATTEMPTS_NAME.to_string(),
            ATTEMPTS_HELP.to_string(),
            vec!["outcome".to_string(), "upstream".to_string()],
            HashMap::new(),
        )?;
        Ok(MetricsCollector {
            observer,
            attempts_desc,
        })
    }
}

impl Collector for MetricsCollector {
    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let snapshots = &self.observer.snapshots();
        if snapshots.is_empty() {
            return vec![];
        }

        let metrics = snapshots
            .iter()
            .flat_map(|(upstream, snapshot)| {
                outcomes(snapshot)
                    .into_iter()
                    .map(|(outcome, value)| counter_metric(upstream.as_str(), outcome, value))
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut family = MetricFamily::default();
        family.set_name(ATTEMPTS_NAME.into());
        family.set_help(ATTEMPTS_HELP.into());
        family.set_field_type(MetricType::COUNTER);
        family.set_metric(metrics);
        vec![family]
    }
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.attempts_desc]
    }
}

fn outcomes(snapshot: &StatsSnapshot) -> [(&'static str, u64); 4] {
    [
        (SUCCESS, snapshot.success),
        (CallError::UNREACHABLE, snapshot.unreachable),
        (CallError::READ_FAILED, snapshot.read_failed),
        (CallError::ERROR_STATUS, snapshot.error_status),
    ]
}

fn label(name: &str, value: &str) -> LabelPair {
    LabelPair {
        name: Some(name.into()),
        value: Some(value.into()),
        ..Default::default()
    }
}

fn counter_metric(upstream: &str, outcome: &str, value: u64) -> Metric {
    Metric {
        label: vec![label("outcome", outcome), label("upstream", upstream)],
        counter: Counter {
            value: Some(value as f64),
            ..Default::default()
        }
        .into(),
        ..Default::default() // gauge: (),
                             // summary: (),
                             // untyped: (),
                             // histogram: (),
                             // timestamp_ms: (),
                             // special_fields: (),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use prometheus::{TextEncoder, core::Collector};
    use reqwest::StatusCode;

    use crate::{
        observer::{MetricsObserver, Observer},
        route::metrics::MetricsCollector,
        upstream::{CallError, CallRecord, UpstreamId},
    };

    const EXPECTED: &str = r#"# HELP rpc_attempts_total Total upstream call attempts by outcome
# TYPE rpc_attempts_total counter
rpc_attempts_total{outcome="success",upstream="alpha"} 1
rpc_attempts_total{outcome="unreachable",upstream="alpha"} 2
rpc_attempts_total{outcome="read_failed",upstream="alpha"} 1
rpc_attempts_total{outcome="error_status",upstream="alpha"} 0
rpc_attempts_total{outcome="success",upstream="zulu"} 0
rpc_attempts_total{outcome="unreachable",upstream="zulu"} 0
rpc_attempts_total{outcome="read_failed",upstream="zulu"} 0
rpc_attempts_total{outcome="error_status",upstream="zulu"} 0
"#;

    #[test]
    fn collect_encodes_expected_lines() {
        let zulu = UpstreamId::new("zulu");
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![zulu.clone(), alpha.clone()]));
        let collector = MetricsCollector::new(observer.clone()).unwrap();

        observer.record(
            &alpha,
            CallRecord {
                outcome: Ok(StatusCode::OK),
                duration: Duration::from_millis(100),
            },
        );

        let read_failed = CallError::ReadFailed {
            error: "EOF".to_string(),
            http_status: StatusCode::OK,
        };
        observer.record(
            &alpha,
            CallRecord {
                outcome: Err(&read_failed),
                duration: Duration::from_millis(150),
            },
        );

        let unreachable = CallError::Unreachable {
            error: "unreachable".to_string(),
        };
        for _ in 0..2 {
            observer.record(
                &alpha,
                CallRecord {
                    outcome: Err(&unreachable),
                    duration: Duration::from_millis(150),
                },
            );
        }

        let actual = TextEncoder::new()
            .encode_to_string(&collector.collect())
            .unwrap();

        assert_eq!(sorted_lines(&actual), sorted_lines(EXPECTED));
    }

    #[test]
    fn test_collect_is_empty_without_upstreams() {
        let observer = Arc::new(MetricsObserver::new(vec![]));
        let collector = MetricsCollector::new(observer.clone()).unwrap();
        assert!(collector.collect().is_empty());
    }

    #[test]
    fn test_desc_advertises_one_metric() {
        let observer = Arc::new(MetricsObserver::new(vec![]));
        let collector = MetricsCollector::new(observer.clone()).unwrap();
        assert!(collector.desc().len() == 1);
    }

    fn sorted_lines(s: &str) -> Vec<&str> {
        let mut lines: Vec<_> = s.lines().filter(|l| !l.trim().is_empty()).collect();
        lines.sort_unstable();
        lines
    }
}
