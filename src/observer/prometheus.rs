use std::{collections::HashMap, sync::Arc};

use prometheus::{
    core::{Collector as PrometheusCollector, Desc},
    proto::{Bucket, Counter, Histogram, LabelPair, Metric, MetricFamily, MetricType},
};

use crate::{
    observer::{
        MetricsObserver,
        snapshot::{
            ATTEMPT_BOUNDS, BUCKET_BOUNDS_MICROS, HedgeSnapshot, RequestSnapshot, Snapshot,
        },
    },
    upstream::call::CallError,
};

const ATTEMPTS_NAME: &str = "rpc_attempts_total";
const ATTEMPTS_HELP: &str = "Total upstream call attempts by outcome";
const ATTEMPTS_DURATION_NAME: &str = "rpc_attempt_duration_seconds";
const ATTEMPTS_DURATION_HELP: &str = "Rpc attempt duration in seconds";
const HEDGES_NAME: &str = "rpc_hedges_total";
const HEDGES_HELP: &str =
    "Hedge calls started because this upstream passed its hedge_after, by the upstream hedged to";
const HEDGE_WINS_NAME: &str = "rpc_hedge_wins_total";
const HEDGE_WINS_HELP: &str =
    "Hedge calls that overtook this upstream and answered first, by the upstream hedged to";
const COALESCED_NAME: &str = "rpc_requests_coalesced_total";
const COALESCED_HELP: &str = "Requests answered by another request's flight";
const REQUESTS_NAME: &str = "rpc_requests_total";
const REQUESTS_HELP: &str = "Requests that ran their own upstream chain, by outcome";
const REQUEST_ATTEMPTS_NAME: &str = "rpc_request_attempts";
const REQUEST_ATTEMPTS_HELP: &str =
    "Upstream attempts per request, hedges included; above 1 means failover or a hedge";
const SUCCESS: &str = "success";
const FAILED: &str = "failed";
const ABANDONED: &str = "abandoned";

const MICROS_PER_SECOND: f64 = 1_000_000.0;

pub struct Collector {
    observer: Arc<MetricsObserver>,
    attempts_desc: Desc,
    attempts_duration_desc: Desc,
    hedges_desc: Desc,
    hedge_wins_desc: Desc,
    coalesced_desc: Desc,
    requests_desc: Desc,
    request_attempts_desc: Desc,
}

impl Collector {
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
        let hedges_desc = pair_desc(HEDGES_NAME, HEDGES_HELP)?;
        let hedge_wins_desc = pair_desc(HEDGE_WINS_NAME, HEDGE_WINS_HELP)?;
        let coalesced_desc = Desc::new(
            COALESCED_NAME.to_string(),
            COALESCED_HELP.to_string(),
            vec![],
            HashMap::new(),
        )?;
        let requests_desc = Desc::new(
            REQUESTS_NAME.to_string(),
            REQUESTS_HELP.to_string(),
            vec!["outcome".to_string()],
            HashMap::new(),
        )?;
        let request_attempts_desc = Desc::new(
            REQUEST_ATTEMPTS_NAME.to_string(),
            REQUEST_ATTEMPTS_HELP.to_string(),
            vec![],
            HashMap::new(),
        )?;
        Ok(Collector {
            observer,
            attempts_desc,
            attempts_duration_desc,
            hedges_desc,
            hedge_wins_desc,
            coalesced_desc,
            requests_desc,
            request_attempts_desc,
        })
    }
}

impl PrometheusCollector for Collector {
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

        let hedges = self.observer.hedge_snapshots();
        let requests = self.observer.requests();
        let per_pair = |count: fn(&HedgeSnapshot) -> u64| {
            hedges
                .iter()
                .map(|(overtaken, to, hedge)| {
                    pair_counter_metric(overtaken.as_str(), to.as_str(), count(hedge))
                })
                .collect()
        };

        vec![
            family(ATTEMPTS_NAME, ATTEMPTS_HELP, MetricType::COUNTER, metrics),
            family(
                ATTEMPTS_DURATION_NAME,
                ATTEMPTS_DURATION_HELP,
                MetricType::HISTOGRAM,
                durations,
            ),
            family(
                HEDGES_NAME,
                HEDGES_HELP,
                MetricType::COUNTER,
                per_pair(|hedge| hedge.started),
            ),
            family(
                HEDGE_WINS_NAME,
                HEDGE_WINS_HELP,
                MetricType::COUNTER,
                per_pair(|hedge| hedge.won),
            ),
            family(
                COALESCED_NAME,
                COALESCED_HELP,
                MetricType::COUNTER,
                vec![unlabelled_counter_metric(self.observer.coalesced())],
            ),
            family(
                REQUESTS_NAME,
                REQUESTS_HELP,
                MetricType::COUNTER,
                vec![
                    outcome_counter_metric(SUCCESS, requests.answered),
                    outcome_counter_metric(FAILED, requests.failed),
                ],
            ),
            family(
                REQUEST_ATTEMPTS_NAME,
                REQUEST_ATTEMPTS_HELP,
                MetricType::HISTOGRAM,
                vec![attempts_histogram_metric(&requests)],
            ),
        ]
    }
    fn desc(&self) -> Vec<&Desc> {
        vec![
            &self.attempts_desc,
            &self.attempts_duration_desc,
            &self.hedges_desc,
            &self.hedge_wins_desc,
            &self.coalesced_desc,
            &self.requests_desc,
            &self.request_attempts_desc,
        ]
    }
}

/// `upstream` is the one overtaken, `to` the one hedged to.
fn pair_desc(name: &str, help: &str) -> prometheus::Result<Desc> {
    Desc::new(
        name.to_string(),
        help.to_string(),
        vec!["upstream".to_string(), "to".to_string()],
        HashMap::new(),
    )
}

fn family(name: &str, help: &str, kind: MetricType, metrics: Vec<Metric>) -> MetricFamily {
    let mut family = MetricFamily::default();
    family.set_name(name.into());
    family.set_help(help.into());
    family.set_field_type(kind);
    family.set_metric(metrics);
    family
}

fn outcomes(snapshot: &Snapshot) -> [(&'static str, u64); 6] {
    [
        (SUCCESS, snapshot.success),
        (CallError::UNREACHABLE, snapshot.unreachable),
        (CallError::READ_FAILED, snapshot.read_failed),
        (CallError::ERROR_STATUS, snapshot.error_status),
        (CallError::RPC_ERROR, snapshot.rpc_error),
        (ABANDONED, snapshot.abandoned),
    ]
}

fn label(name: &str, value: &str) -> LabelPair {
    LabelPair {
        name: Some(name.into()),
        value: Some(value.into()),
        ..Default::default()
    }
}

fn histogram_metric(upstream: &str, snapshot: &Snapshot) -> Metric {
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

fn attempts_histogram_metric(requests: &RequestSnapshot) -> Metric {
    let buckets = ATTEMPT_BOUNDS
        .iter()
        .zip(cumulative(requests.attempt_buckets))
        .map(|(bound, count)| Bucket {
            upper_bound: Some(*bound as f64),
            cumulative_count: Some(count),
            ..Default::default()
        })
        .collect();

    Metric {
        histogram: Histogram {
            sample_count: Some(requests.answered + requests.failed),
            sample_sum: Some(requests.attempts_total as f64),
            bucket: buckets,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn cumulative<const N: usize>(buckets: [u64; N]) -> [u64; N] {
    let mut running = 0;
    buckets.map(|count| {
        running += count;
        running
    })
}

fn attempts(snapshot: &Snapshot) -> u64 {
    outcomes(snapshot).into_iter().map(|(_, count)| count).sum()
}

fn pair_counter_metric(upstream: &str, to: &str, value: u64) -> Metric {
    Metric {
        label: vec![label("upstream", upstream), label("to", to)],
        counter: Counter {
            value: Some(value as f64),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn outcome_counter_metric(outcome: &str, value: u64) -> Metric {
    Metric {
        label: vec![label("outcome", outcome)],
        counter: Counter {
            value: Some(value as f64),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn unlabelled_counter_metric(value: u64) -> Metric {
    Metric {
        counter: Counter {
            value: Some(value as f64),
            ..Default::default()
        }
        .into(),
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

    use prometheus::{TextEncoder, core::Collector as PrometheusCollector, proto::MetricFamily};
    use reqwest::StatusCode;

    use crate::{
        observer::{
            MetricsObserver, Observer,
            prometheus::{
                ATTEMPTS_DURATION_NAME, ATTEMPTS_NAME, COALESCED_NAME, Collector, HEDGE_WINS_NAME,
                HEDGES_NAME, REQUEST_ATTEMPTS_NAME, REQUESTS_NAME, cumulative,
            },
        },
        upstream::{
            UpstreamId,
            call::{CallError, CallRecord},
        },
    };

    const EXPECTED: &str = r#"# HELP rpc_attempts_total Total upstream call attempts by outcome
# TYPE rpc_attempts_total counter
rpc_attempts_total{outcome="success",upstream="alpha"} 1
rpc_attempts_total{outcome="unreachable",upstream="alpha"} 2
rpc_attempts_total{outcome="read_failed",upstream="alpha"} 1
rpc_attempts_total{outcome="error_status",upstream="alpha"} 0
rpc_attempts_total{outcome="rpc_error",upstream="alpha"} 1
rpc_attempts_total{outcome="abandoned",upstream="alpha"} 1
rpc_attempts_total{outcome="success",upstream="zulu"} 0
rpc_attempts_total{outcome="unreachable",upstream="zulu"} 0
rpc_attempts_total{outcome="read_failed",upstream="zulu"} 0
rpc_attempts_total{outcome="error_status",upstream="zulu"} 0
rpc_attempts_total{outcome="rpc_error",upstream="zulu"} 0
rpc_attempts_total{outcome="abandoned",upstream="zulu"} 0
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
        let collector = Collector::new(observer.clone()).unwrap();

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

        // A rate limit arriving on an HTTP 200. It must land on its own series,
        // not on `success`.
        let rpc_error = CallError::RpcError {
            http_status: StatusCode::OK,
            code: -32005,
            retryable: true,
        };
        observer.record(
            &alpha,
            CallRecord {
                outcome: Err(&rpc_error),
                duration: Duration::from_millis(150),
            },
        );

        observer.record_abandoned(&alpha, Duration::from_millis(150));

        let actual = encode(&collector.collect(), ATTEMPTS_NAME);

        assert_eq!(sorted_lines(&actual), sorted_lines(EXPECTED));
    }

    #[test]
    fn test_collect_is_empty_without_upstreams() {
        let observer = Arc::new(MetricsObserver::new(vec![]));
        let collector = Collector::new(observer.clone()).unwrap();
        assert!(collector.collect().is_empty());
    }

    /// `upstream` is the slow one, `to` the one that stepped in.
    const EXPECTED_HEDGES: &str = r#"# HELP rpc_hedges_total Hedge calls started because this upstream passed its hedge_after, by the upstream hedged to
# TYPE rpc_hedges_total counter
rpc_hedges_total{upstream="alpha",to="zulu"} 2
rpc_hedges_total{upstream="zulu",to="alpha"} 0
"#;

    const EXPECTED_HEDGE_WINS: &str = r#"# HELP rpc_hedge_wins_total Hedge calls that overtook this upstream and answered first, by the upstream hedged to
# TYPE rpc_hedge_wins_total counter
rpc_hedge_wins_total{upstream="alpha",to="zulu"} 1
rpc_hedge_wins_total{upstream="zulu",to="alpha"} 0
"#;

    #[test]
    fn hedges_and_wins_are_counted_per_pair() {
        let zulu = UpstreamId::new("zulu");
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![zulu.clone(), alpha.clone()]));
        let collector = Collector::new(observer.clone()).unwrap();

        observer.record_hedge(&alpha, &zulu);
        observer.record_hedge(&alpha, &zulu);
        observer.record_hedge_win(&alpha, &zulu);

        let families = collector.collect();

        assert_eq!(encode(&families, HEDGES_NAME), EXPECTED_HEDGES);
        assert_eq!(encode(&families, HEDGE_WINS_NAME), EXPECTED_HEDGE_WINS);
    }

    const EXPECTED_COALESCED: &str = r#"# HELP rpc_requests_coalesced_total Requests answered by another request's flight
# TYPE rpc_requests_coalesced_total counter
rpc_requests_coalesced_total 2
"#;

    #[test]
    fn coalesced_requests_are_one_unlabelled_counter() {
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![alpha]));
        let collector = Collector::new(observer.clone()).unwrap();

        observer.record_coalesced();
        observer.record_coalesced();

        assert_eq!(
            encode(&collector.collect(), COALESCED_NAME),
            EXPECTED_COALESCED
        );
    }

    #[test]
    fn coalesced_reads_zero_before_any_flight() {
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![alpha]));
        let collector = Collector::new(observer).unwrap();

        assert!(
            encode(&collector.collect(), COALESCED_NAME).contains("rpc_requests_coalesced_total 0")
        );
    }

    #[test]
    fn test_desc_advertises_every_family() {
        let observer = Arc::new(MetricsObserver::new(vec![]));
        let collector = Collector::new(observer.clone()).unwrap();

        let names: Vec<&str> = collector
            .desc()
            .iter()
            .map(|desc| desc.fq_name.as_str())
            .collect();

        assert_eq!(
            names,
            [
                ATTEMPTS_NAME,
                ATTEMPTS_DURATION_NAME,
                HEDGES_NAME,
                HEDGE_WINS_NAME,
                COALESCED_NAME,
                REQUESTS_NAME,
                REQUEST_ATTEMPTS_NAME,
            ]
        );
    }

    const EXPECTED_REQUESTS: &str = r#"# HELP rpc_requests_total Requests that ran their own upstream chain, by outcome
# TYPE rpc_requests_total counter
rpc_requests_total{outcome="success"} 2
rpc_requests_total{outcome="failed"} 1
"#;

    /// One request per path: answered first try, failed over once, exhausted 3.
    const EXPECTED_REQUEST_ATTEMPTS: &str = r#"# HELP rpc_request_attempts Upstream attempts per request, hedges included; above 1 means failover or a hedge
# TYPE rpc_request_attempts histogram
rpc_request_attempts_bucket{le="1"} 1
rpc_request_attempts_bucket{le="2"} 2
rpc_request_attempts_bucket{le="3"} 3
rpc_request_attempts_bucket{le="5"} 3
rpc_request_attempts_bucket{le="10"} 3
rpc_request_attempts_bucket{le="+Inf"} 3
rpc_request_attempts_sum 6
rpc_request_attempts_count 3
"#;

    #[test]
    fn requests_render_by_outcome_and_attempts() {
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![alpha]));
        let collector = Collector::new(observer.clone()).unwrap();

        observer.record_request(true, 1);
        observer.record_request(true, 2);
        observer.record_request(false, 3);

        let families = collector.collect();

        assert_eq!(encode(&families, REQUESTS_NAME), EXPECTED_REQUESTS);
        assert_eq!(
            encode(&families, REQUEST_ATTEMPTS_NAME),
            EXPECTED_REQUEST_ATTEMPTS
        );
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
        let collector = Collector::new(observer.clone()).unwrap();

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
        let collector = Collector::new(observer.clone()).unwrap();

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

    fn sorted_lines(s: &str) -> Vec<&str> {
        let mut lines: Vec<_> = s.lines().filter(|l| !l.trim().is_empty()).collect();
        lines.sort_unstable();
        lines
    }
}
