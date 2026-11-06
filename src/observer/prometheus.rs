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
const FAILOVERS_NAME: &str = "rpc_failovers_total";
const FAILOVERS_HELP: &str = "Times the chain moved past this upstream after a retryable failure";
const COALESCED_NAME: &str = "rpc_requests_coalesced_total";
const COALESCED_HELP: &str = "Requests answered by another request's flight";
const REQUESTS_NAME: &str = "rpc_requests_total";
const REQUESTS_HELP: &str = "Requests that ran their own upstream chain, by method and outcome";
const REQUEST_ATTEMPTS_NAME: &str = "rpc_request_attempts";
const REQUEST_ATTEMPTS_HELP: &str =
    "Upstream attempts per request, hedges included; above 1 means failover or a hedge";
const REQUEST_DURATION_NAME: &str = "rpc_request_duration_seconds";
const REQUEST_DURATION_HELP: &str =
    "Time from receiving a request to its answer, failovers and hedges included, by method";
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
    failovers_desc: Desc,
    coalesced_desc: Desc,
    requests_desc: Desc,
    request_attempts_desc: Desc,
    request_duration_desc: Desc,
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
        let failovers_desc = Desc::new(
            FAILOVERS_NAME.to_string(),
            FAILOVERS_HELP.to_string(),
            vec!["upstream".to_string()],
            HashMap::new(),
        )?;
        let coalesced_desc = Desc::new(
            COALESCED_NAME.to_string(),
            COALESCED_HELP.to_string(),
            vec![],
            HashMap::new(),
        )?;
        let requests_desc = Desc::new(
            REQUESTS_NAME.to_string(),
            REQUESTS_HELP.to_string(),
            vec!["method".to_string(), "outcome".to_string()],
            HashMap::new(),
        )?;
        let request_attempts_desc = Desc::new(
            REQUEST_ATTEMPTS_NAME.to_string(),
            REQUEST_ATTEMPTS_HELP.to_string(),
            vec!["method".to_string()],
            HashMap::new(),
        )?;
        let request_duration_desc = Desc::new(
            REQUEST_DURATION_NAME.to_string(),
            REQUEST_DURATION_HELP.to_string(),
            vec!["method".to_string()],
            HashMap::new(),
        )?;
        Ok(Collector {
            observer,
            attempts_desc,
            attempts_duration_desc,
            hedges_desc,
            hedge_wins_desc,
            failovers_desc,
            coalesced_desc,
            requests_desc,
            request_attempts_desc,
            request_duration_desc,
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
        let requests: Vec<_> = self
            .observer
            .requests()
            .into_iter()
            .filter(|(_, request)| request.answered + request.failed > 0)
            .collect();
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
                FAILOVERS_NAME,
                FAILOVERS_HELP,
                MetricType::COUNTER,
                self.observer
                    .failovers()
                    .into_iter()
                    .map(|(upstream, count)| upstream_counter_metric(upstream.as_str(), count))
                    .collect(),
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
                requests
                    .iter()
                    .flat_map(|(method, request)| {
                        [
                            request_counter_metric(method, SUCCESS, request.answered),
                            request_counter_metric(method, FAILED, request.failed),
                        ]
                    })
                    .collect(),
            ),
            family(
                REQUEST_ATTEMPTS_NAME,
                REQUEST_ATTEMPTS_HELP,
                MetricType::HISTOGRAM,
                requests
                    .iter()
                    .map(|(method, request)| attempts_histogram_metric(method, request))
                    .collect(),
            ),
            family(
                REQUEST_DURATION_NAME,
                REQUEST_DURATION_HELP,
                MetricType::HISTOGRAM,
                requests
                    .iter()
                    .map(|(method, request)| {
                        duration_histogram_metric(
                            label("method", method),
                            request.duration_buckets,
                            request.answered + request.failed,
                            request.duration_micros_total,
                        )
                    })
                    .collect(),
            ),
        ]
    }
    fn desc(&self) -> Vec<&Desc> {
        vec![
            &self.attempts_desc,
            &self.attempts_duration_desc,
            &self.hedges_desc,
            &self.hedge_wins_desc,
            &self.failovers_desc,
            &self.coalesced_desc,
            &self.requests_desc,
            &self.request_attempts_desc,
            &self.request_duration_desc,
        ]
    }
}

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

fn outcomes(snapshot: &Snapshot) -> [(&'static str, u64); 7] {
    [
        (SUCCESS, snapshot.success),
        (CallError::UNREACHABLE, snapshot.unreachable),
        (CallError::READ_FAILED, snapshot.read_failed),
        (CallError::ERROR_STATUS, snapshot.error_status),
        (CallError::RPC_ERROR, snapshot.rpc_error),
        (CallError::RPC_CALLER_FAULT, snapshot.rpc_caller_fault),
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
    duration_histogram_metric(
        label("upstream", upstream),
        snapshot.buckets,
        attempts(snapshot),
        snapshot.duration_micros_total,
    )
}

fn duration_histogram_metric(
    label: LabelPair,
    buckets: [u64; BUCKET_BOUNDS_MICROS.len()],
    count: u64,
    total_micros: u64,
) -> Metric {
    let buckets = BUCKET_BOUNDS_MICROS
        .iter()
        .zip(cumulative(buckets))
        .map(|(bound_micros, count)| Bucket {
            upper_bound: Some(*bound_micros as f64 / MICROS_PER_SECOND),
            cumulative_count: Some(count),
            ..Default::default()
        })
        .collect();

    Metric {
        label: vec![label],
        histogram: Histogram {
            sample_count: Some(count),
            sample_sum: Some(total_micros as f64 / MICROS_PER_SECOND),
            bucket: buckets,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn attempts_histogram_metric(method: &str, requests: &RequestSnapshot) -> Metric {
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
        label: vec![label("method", method)],
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

fn request_counter_metric(method: &str, outcome: &str, value: u64) -> Metric {
    Metric {
        label: vec![label("method", method), label("outcome", outcome)],
        counter: Counter {
            value: Some(value as f64),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn upstream_counter_metric(upstream: &str, value: u64) -> Metric {
    Metric {
        label: vec![label("upstream", upstream)],
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
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::body::Bytes;
    use prometheus::{TextEncoder, core::Collector as PrometheusCollector, proto::MetricFamily};
    use reqwest::StatusCode;

    use crate::{
        observer::{
            MetricsObserver, Observer,
            prometheus::{
                ATTEMPTS_DURATION_NAME, ATTEMPTS_NAME, COALESCED_NAME, Collector, FAILOVERS_NAME,
                HEDGE_WINS_NAME, HEDGES_NAME, REQUEST_ATTEMPTS_NAME, REQUEST_DURATION_NAME,
                REQUESTS_NAME,
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
rpc_attempts_total{outcome="rpc_caller_fault",upstream="alpha"} 0
rpc_attempts_total{outcome="abandoned",upstream="alpha"} 1
rpc_attempts_total{outcome="success",upstream="zulu"} 0
rpc_attempts_total{outcome="unreachable",upstream="zulu"} 0
rpc_attempts_total{outcome="read_failed",upstream="zulu"} 0
rpc_attempts_total{outcome="error_status",upstream="zulu"} 0
rpc_attempts_total{outcome="rpc_error",upstream="zulu"} 0
rpc_attempts_total{outcome="rpc_caller_fault",upstream="zulu"} 0
rpc_attempts_total{outcome="abandoned",upstream="zulu"} 0
"#;

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

        let rpc_error = CallError::RpcError {
            http_status: StatusCode::OK,
            code: -32005,
            retryable: true,
            body: Bytes::new(),
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

    const EXPECTED_FAILOVERS: &str = r#"# HELP rpc_failovers_total Times the chain moved past this upstream after a retryable failure
# TYPE rpc_failovers_total counter
rpc_failovers_total{upstream="alpha"} 2
rpc_failovers_total{upstream="zulu"} 0
"#;

    #[test]
    fn failovers_are_counted_per_upstream_left_behind() {
        let zulu = UpstreamId::new("zulu");
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![zulu, alpha.clone()]));
        let collector = Collector::new(observer.clone()).unwrap();

        observer.record_failover(&alpha);
        observer.record_failover(&alpha);

        assert_eq!(
            encode(&collector.collect(), FAILOVERS_NAME),
            EXPECTED_FAILOVERS
        );
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
    fn desc_advertises_exactly_the_families_collect_emits() {
        let observer = Arc::new(MetricsObserver::new(vec![
            UpstreamId::new("alpha"),
            UpstreamId::new("zulu"),
        ]));
        let collector = Collector::new(observer.clone()).unwrap();

        let mut advertised: Vec<String> = collector
            .desc()
            .iter()
            .map(|desc| desc.fq_name.clone())
            .collect();
        let mut emitted: Vec<String> = collector
            .collect()
            .iter()
            .map(|family| family.name().to_string())
            .collect();
        advertised.sort();
        emitted.sort();

        assert_eq!(advertised, emitted);
    }

    const EXPECTED_REQUESTS: &str = r#"# HELP rpc_requests_total Requests that ran their own upstream chain, by method and outcome
# TYPE rpc_requests_total counter
rpc_requests_total{method="eth_call",outcome="success"} 2
rpc_requests_total{method="eth_call",outcome="failed"} 1
rpc_requests_total{method="other",outcome="success"} 0
rpc_requests_total{method="other",outcome="failed"} 1
"#;

    const EXPECTED_REQUEST_ATTEMPTS: &str = r#"# HELP rpc_request_attempts Upstream attempts per request, hedges included; above 1 means failover or a hedge
# TYPE rpc_request_attempts histogram
rpc_request_attempts_bucket{method="eth_call",le="1"} 1
rpc_request_attempts_bucket{method="eth_call",le="2"} 2
rpc_request_attempts_bucket{method="eth_call",le="3"} 3
rpc_request_attempts_bucket{method="eth_call",le="5"} 3
rpc_request_attempts_bucket{method="eth_call",le="10"} 3
rpc_request_attempts_bucket{method="eth_call",le="+Inf"} 3
rpc_request_attempts_sum{method="eth_call"} 6
rpc_request_attempts_count{method="eth_call"} 3
rpc_request_attempts_bucket{method="other",le="1"} 1
rpc_request_attempts_bucket{method="other",le="2"} 1
rpc_request_attempts_bucket{method="other",le="3"} 1
rpc_request_attempts_bucket{method="other",le="5"} 1
rpc_request_attempts_bucket{method="other",le="10"} 1
rpc_request_attempts_bucket{method="other",le="+Inf"} 1
rpc_request_attempts_sum{method="other"} 1
rpc_request_attempts_count{method="other"} 1
"#;

    const EXPECTED_REQUEST_DURATION: &str = r#"# HELP rpc_request_duration_seconds Time from receiving a request to its answer, failovers and hedges included, by method
# TYPE rpc_request_duration_seconds histogram
rpc_request_duration_seconds_bucket{method="eth_call",le="0.005"} 0
rpc_request_duration_seconds_bucket{method="eth_call",le="0.01"} 0
rpc_request_duration_seconds_bucket{method="eth_call",le="0.025"} 0
rpc_request_duration_seconds_bucket{method="eth_call",le="0.05"} 1
rpc_request_duration_seconds_bucket{method="eth_call",le="0.1"} 2
rpc_request_duration_seconds_bucket{method="eth_call",le="0.25"} 2
rpc_request_duration_seconds_bucket{method="eth_call",le="0.5"} 2
rpc_request_duration_seconds_bucket{method="eth_call",le="1"} 2
rpc_request_duration_seconds_bucket{method="eth_call",le="3"} 2
rpc_request_duration_seconds_bucket{method="eth_call",le="5"} 3
rpc_request_duration_seconds_bucket{method="eth_call",le="+Inf"} 3
rpc_request_duration_seconds_sum{method="eth_call"} 3.32
rpc_request_duration_seconds_count{method="eth_call"} 3
rpc_request_duration_seconds_bucket{method="other",le="0.005"} 1
rpc_request_duration_seconds_bucket{method="other",le="0.01"} 1
rpc_request_duration_seconds_bucket{method="other",le="0.025"} 1
rpc_request_duration_seconds_bucket{method="other",le="0.05"} 1
rpc_request_duration_seconds_bucket{method="other",le="0.1"} 1
rpc_request_duration_seconds_bucket{method="other",le="0.25"} 1
rpc_request_duration_seconds_bucket{method="other",le="0.5"} 1
rpc_request_duration_seconds_bucket{method="other",le="1"} 1
rpc_request_duration_seconds_bucket{method="other",le="3"} 1
rpc_request_duration_seconds_bucket{method="other",le="5"} 1
rpc_request_duration_seconds_bucket{method="other",le="+Inf"} 1
rpc_request_duration_seconds_sum{method="other"} 0.002
rpc_request_duration_seconds_count{method="other"} 1
"#;

    #[test]
    fn requests_render_by_outcome_and_attempts() {
        let alpha = UpstreamId::new("alpha");
        let observer = Arc::new(MetricsObserver::new(vec![alpha]));
        let collector = Collector::new(observer.clone()).unwrap();

        observer.record_request("eth_call", true, 1, Duration::from_millis(40));
        observer.record_request("eth_call", true, 2, Duration::from_millis(80));
        observer.record_request("eth_call", false, 3, Duration::from_millis(3200));
        observer.record_request("other", false, 1, Duration::from_millis(2));

        let families = collector.collect();

        assert_eq!(encode(&families, REQUESTS_NAME), EXPECTED_REQUESTS);
        assert_eq!(
            encode(&families, REQUEST_ATTEMPTS_NAME),
            EXPECTED_REQUEST_ATTEMPTS
        );
        assert_eq!(
            encode(&families, REQUEST_DURATION_NAME),
            EXPECTED_REQUEST_DURATION
        );
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
                duration: Duration::from_secs(6),
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
