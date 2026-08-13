use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use prometheus::{
    Encoder, Registry, TextEncoder,
    core::{Collector, Desc},
    proto::{Bucket, Counter, Histogram, LabelPair, Metric, MetricFamily, MetricType},
};
use tracing::error;

use crate::{
    observer::{BUCKET_BOUNDS_MICROS, MetricsObserver, StatsSnapshot},
    upstream::CallError,
};

const ATTEMPTS_NAME: &str = "rpc_attempts_total";
const ATTEMPTS_HELP: &str = "Total upstream call attempts by outcome";
const ATTEMPTS_DURATION_NAME: &str = "rpc_attempt_duration_seconds";
const ATTEMPTS_DURATION_HELP: &str = "Rpc attempt duration in seconds";
const SUCCESS: &str = "success";

const MICROS_PER_SECOND: f64 = 1_000_000.0;

pub struct MetricsCollector {
    observer: Arc<MetricsObserver>,
    attempts_desc: Desc,
    attempts_duration_desc: Desc,
}

impl MetricsCollector {
    pub fn new(observer: Arc<MetricsObserver>) -> prometheus::Result<Self> {
        let attempts_desc = Desc::new(
            ATTEMPTS_NAME.to_string(),
            ATTEMPTS_HELP.to_string(),
            vec!["outcome".to_string(), "upstream".to_string()],
            HashMap::new(),
        )?;
        let attempts_duration_desc = Desc::new(
            ATTEMPTS_DURATION_NAME.to_string(),
            ATTEMPTS_DURATION_HELP.to_string(),
            vec!["upstream".to_string()],
            HashMap::new(),
        )?;
        Ok(MetricsCollector {
            observer,
            attempts_desc,
            attempts_duration_desc,
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

        let durations = snapshots
            .iter()
            .map(|(upstream, snapshot)| histogram_metric(upstream.as_str(), snapshot))
            .collect();

        vec![
            family(ATTEMPTS_NAME, ATTEMPTS_HELP, MetricType::COUNTER, metrics),
            family(
                ATTEMPTS_DURATION_NAME,
                ATTEMPTS_DURATION_HELP,
                MetricType::HISTOGRAM,
                durations,
            ),
        ]
    }
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.attempts_desc, &self.attempts_duration_desc]
    }
}

fn family(name: &str, help: &str, kind: MetricType, metrics: Vec<Metric>) -> MetricFamily {
    let mut family = MetricFamily::default();
    family.set_name(name.into());
    family.set_help(help.into());
    family.set_field_type(kind);
    family.set_metric(metrics);
    family
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

fn histogram_metric(upstream: &str, snapshot: &StatsSnapshot) -> Metric {
    let buckets = BUCKET_BOUNDS_MICROS
        .iter()
        .zip(cumulative(snapshot.buckets))
        .map(|(bound_micros, count)| Bucket {
            upper_bound: Some(*bound_micros as f64 / MICROS_PER_SECOND),
            cumulative_count: Some(count),
            ..Default::default()
        })
        .collect();

    Metric {
        label: vec![label("upstream", upstream)],
        histogram: Histogram {
            sample_count: Some(attempts(snapshot)),
            sample_sum: Some(snapshot.duration_micros_total as f64 / MICROS_PER_SECOND),
            bucket: buckets,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn cumulative(buckets: [u64; BUCKET_BOUNDS_MICROS.len()]) -> [u64; BUCKET_BOUNDS_MICROS.len()] {
    let mut running = 0;
    buckets.map(|count| {
        running += count;
        running
    })
}

fn attempts(snapshot: &StatsSnapshot) -> u64 {
    outcomes(snapshot).into_iter().map(|(_, count)| count).sum()
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

pub async fn get_metrics(State(registry): State<Arc<Registry>>) -> Response {
    let encoder = TextEncoder::new();

    match encoder.encode_to_string(&registry.gather()) {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, encoder.format_type())],
            body,
        )
            .into_response(),
        Err(err) => {
            error!(event = "metrics_render_failed", error = %err);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::extract::State;
    use prometheus::{Encoder, Registry, TextEncoder, core::Collector, proto::MetricFamily};
    use reqwest::StatusCode;

    use crate::{
        observer::{MetricsObserver, Observer},
        route::metrics::{
            ATTEMPTS_DURATION_NAME, ATTEMPTS_NAME, MetricsCollector, cumulative, get_metrics,
        },
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

    /// One 100ms attempt: `BUCKET_BOUNDS_MICROS` in seconds, cumulative from the
    /// slot it landed in, and a `+Inf` line the collector never emitted.
    const EXPECTED_DURATION: &str = r#"# HELP rpc_attempt_duration_seconds Rpc attempt duration in seconds
# TYPE rpc_attempt_duration_seconds histogram
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="0.005"} 0
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="0.01"} 0
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="0.025"} 0
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="0.05"} 0
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="0.1"} 1
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="0.25"} 1
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="0.5"} 1
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="1"} 1
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="3"} 1
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="5"} 1
rpc_attempt_duration_seconds_bucket{upstream="alpha",le="+Inf"} 1
rpc_attempt_duration_seconds_sum{upstream="alpha"} 0.1
rpc_attempt_duration_seconds_count{upstream="alpha"} 1
"#;

    /// One family at a time: each test asserts its own metric's exposition.
    fn encode(families: &[MetricFamily], name: &str) -> String {
        let wanted: Vec<MetricFamily> = families
            .iter()
            .filter(|family| family.name() == name)
            .cloned()
            .collect();

        TextEncoder::new().encode_to_string(&wanted).unwrap()
    }

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

        let actual = encode(&collector.collect(), ATTEMPTS_NAME);

        assert_eq!(sorted_lines(&actual), sorted_lines(EXPECTED));
    }

    #[test]
    fn test_collect_is_empty_without_upstreams() {
        let observer = Arc::new(MetricsObserver::new(vec![]));
        let collector = MetricsCollector::new(observer.clone()).unwrap();
        assert!(collector.collect().is_empty());
    }

    #[test]
    fn test_desc_advertises_both_families() {
        let observer = Arc::new(MetricsObserver::new(vec![]));
        let collector = MetricsCollector::new(observer.clone()).unwrap();

        let names: Vec<&str> = collector
            .desc()
            .iter()
            .map(|desc| desc.fq_name.as_str())
            .collect();

        assert_eq!(names, [ATTEMPTS_NAME, ATTEMPTS_DURATION_NAME]);
    }

    #[test]
    fn per_slot_counts_accumulate_into_le_totals() {
        let slots = [1, 0, 2, 0, 0, 3, 0, 0, 0, 1];

        assert_eq!(cumulative(slots), [1, 1, 3, 3, 3, 6, 6, 6, 6, 7]);
    }

    #[test]
    fn the_duration_ladder_renders_in_seconds() {
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![alpha.clone()]));
        let collector = MetricsCollector::new(observer.clone()).unwrap();

        observer.record(
            &alpha,
            CallRecord {
                outcome: Ok(StatusCode::OK),
                duration: Duration::from_millis(100),
            },
        );

        let actual = encode(&collector.collect(), ATTEMPTS_DURATION_NAME);

        assert_eq!(actual, EXPECTED_DURATION);
    }

    #[test]
    fn the_inf_bucket_is_derived_and_matches_the_count() {
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![alpha.clone()]));
        let collector = MetricsCollector::new(observer.clone()).unwrap();

        observer.record(
            &alpha,
            CallRecord {
                outcome: Ok(StatusCode::OK),
                duration: Duration::from_millis(100),
            },
        );

        let unreachable = CallError::Unreachable {
            error: "unreachable".to_string(),
        };
        observer.record(
            &alpha,
            CallRecord {
                outcome: Err(&unreachable),
                duration: Duration::from_secs(6), // past the 5.0 ceiling
            },
        );

        let actual = encode(&collector.collect(), ATTEMPTS_DURATION_NAME);

        let top = sample(&actual, r#"_bucket{upstream="alpha",le="5"}"#);
        let inf = sample(&actual, r#"_bucket{upstream="alpha",le="+Inf"}"#);
        let count = sample(&actual, r#"_count{upstream="alpha"}"#);
        let sum = sample(&actual, r#"_sum{upstream="alpha"}"#);

        assert_eq!(inf, count, "+Inf must carry every attempt");
        assert_eq!(count, 2.0);
        assert!(inf > top, "the over-ceiling attempt has no finite bucket");
        assert_eq!(sum, 6.1, "seconds, summed across both attempts");
    }

    fn sample(text: &str, suffix: &str) -> f64 {
        let line = text
            .lines()
            .find(|line| line.split(' ').next().is_some_and(|s| s.ends_with(suffix)))
            .unwrap_or_else(|| panic!("no sample line ending in {suffix}:\n{text}"));

        line.rsplit_once(' ')
            .expect("a sample line carries a value")
            .1
            .parse()
            .expect("the value parses as f64")
    }

    #[tokio::test]
    async fn get_metrics_answers_ok_with_the_exposition_content_type() {
        let registry = Arc::new(Registry::new());

        let response = get_metrics(State(registry)).await;

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            TextEncoder::new().format_type()
        );
    }

    fn sorted_lines(s: &str) -> Vec<&str> {
        let mut lines: Vec<_> = s.lines().filter(|l| !l.trim().is_empty()).collect();
        lines.sort_unstable();
        lines
    }
}
