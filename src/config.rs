use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use anyhow::{Context, Result};
use config::{Config, Source};

const DEFAULT_CONFIG_FILE: &str = "settings.yaml";
const CONFIG_PATH_ENV: &str = "RPC_CONFIG_PATH";

#[derive(serde::Deserialize)]
pub struct ProxySettings {
    pub max_attempt: u64,
    pub retry_after_in_secs: u64,
    pub rpc_timeout_in_secs: u64,
}
#[derive(serde::Deserialize)]
pub struct ApplicationSettings {
    pub port: u16,
    pub host: String,
    pub proxy: ProxySettings,
}
#[derive(serde::Deserialize)]
pub struct Settings {
    pub application: ApplicationSettings,
    #[serde(alias = "rpcs")]
    pub upstreams: Vec<UpstreamSettings>,
    /// Deprecated spelling of `application.proxy.rpc_timeout_in_secs`.
    #[serde(default, rename = "rpc_timeout_in_secs")]
    pub legacy_rpc_timeout_in_secs: Option<u64>,
    pub decider: DeciderKind,
}

#[derive(serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeciderKind {
    RoundRobin,
    PreferLeastErrors,
}

impl DeciderKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeciderKind::RoundRobin => "ROUND_ROBIN",
            DeciderKind::PreferLeastErrors => "PREFER_LEAST_ERRORS",
        }
    }
}

#[derive(serde::Deserialize)]
pub struct UpstreamSettings {
    pub label: String,
    #[serde(alias = "rpc_url")]
    pub url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    #[error("environment variable {0} is unset")]
    UnsetEnvVar(String),
    #[error("upstream {label} still holds an unexpanded ${{...}} placeholder")]
    ResidualPlaceholder { label: String },
    #[error("upstreams must not be empty")]
    EmptyUpstreams,
    #[error("upstreams[{index}] has an empty label")]
    EmptyLabel { index: usize },
    #[error("upstream {label} has an empty url")]
    EmptyUrl { label: String },
    #[error("duplicate label {0}")]
    DuplicateLabel(String),
    #[error("upstreams {first} and {second} share an url")]
    DuplicateUrl { first: String, second: String },
}

pub fn get_settings() -> Result<Settings> {
    let path = std::env::var(CONFIG_PATH_ENV).ok();
    let source = match path.as_deref() {
        Some(path) => config::File::from(Path::new(path)),
        None => config::File::with_name(DEFAULT_CONFIG_FILE),
    };

    settings_from(source, &|key| std::env::var(key).ok())
}

fn settings_from<S>(source: S, get: &dyn Fn(&str) -> Option<String>) -> Result<Settings>
where
    S: Source + Send + Sync + 'static,
{
    let mut settings = Config::builder()
        .set_default("application.host", "127.0.0.1")?
        .set_default("application.proxy.max_attempt", 3)?
        .set_default("application.proxy.retry_after_in_secs", 1)?
        .set_default("application.proxy.rpc_timeout_in_secs", 3)?
        .set_default("decider", "ROUND_ROBIN")?
        .add_source(source)
        .build()?
        .try_deserialize::<Settings>()
        .context("invalid settings")?;

    apply_legacy_keys(&mut settings);
    expand_env(&mut settings, get).context("invalid settings")?;
    validate_settings(&settings).context("invalid settings")?;

    Ok(settings)
}

fn apply_legacy_keys(settings: &mut Settings) {
    if let Some(rpc_timeout_in_secs) = settings.legacy_rpc_timeout_in_secs {
        tracing::warn!(
            event = "config_key_deprecated",
            key = "rpc_timeout_in_secs",
            replacement = "application.proxy.rpc_timeout_in_secs",
        );
        settings.application.proxy.rpc_timeout_in_secs = rpc_timeout_in_secs;
    }
}

fn expand_env(
    settings: &mut Settings,
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<(), SettingsError> {
    settings.application.host = expand_str(&settings.application.host, get)?;

    for rpc in &mut settings.upstreams {
        rpc.label = expand_str(&rpc.label, get)?;
        rpc.url = expand_str(&rpc.url, get)?;
    }

    Ok(())
}

fn expand_str(raw: &str, get: &dyn Fn(&str) -> Option<String>) -> Result<String, SettingsError> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;

    while let Some(start) = rest.find("${") {
        let (before, marker) = rest.split_at(start);
        let body = &marker[2..];
        let Some(end) = body.find('}') else {
            break;
        };

        out.push_str(before);

        let name = &body[..end];
        if name.trim().is_empty() {
            out.push_str(&marker[..end + 3]);
        } else {
            match get(name) {
                Some(value) if !value.is_empty() => out.push_str(&value),
                _ => return Err(SettingsError::UnsetEnvVar(name.to_string())),
            }
        }

        rest = &body[end + 1..];
    }

    out.push_str(rest);
    Ok(out)
}

fn validate_settings(settings: &Settings) -> Result<(), SettingsError> {
    if settings.upstreams.is_empty() {
        return Err(SettingsError::EmptyUpstreams);
    }

    let mut labels: HashSet<&str> = HashSet::new();
    let mut urls: HashMap<&str, &str> = HashMap::new();

    for (index, rpc) in settings.upstreams.iter().enumerate() {
        if rpc.label.trim().is_empty() {
            return Err(SettingsError::EmptyLabel { index });
        }
        if rpc.url.trim().is_empty() {
            return Err(SettingsError::EmptyUrl {
                label: rpc.label.clone(),
            });
        }
        if rpc.label.contains("${") || rpc.url.contains("${") {
            return Err(SettingsError::ResidualPlaceholder {
                label: rpc.label.clone(),
            });
        }
        if !labels.insert(rpc.label.as_str()) {
            return Err(SettingsError::DuplicateLabel(rpc.label.clone()));
        }
        if let Some(first) = urls.insert(rpc.url.as_str(), rpc.label.as_str()) {
            return Err(SettingsError::DuplicateUrl {
                first: first.to_string(),
                second: rpc.label.clone(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{File, FileFormat};

    /// Only the two required keys, so every optional falls through to its default.
    const MINIMAL: &str = r#"
upstreams:
  - label: one
    url: http://127.0.0.1:9001
application:
  port: 8080
"#;

    fn parse(yaml: &str) -> Result<Settings> {
        parse_with_env(yaml, &[])
    }

    fn parse_with_env(yaml: &str, env: &[(&str, &str)]) -> Result<Settings> {
        let env: HashMap<String, String> = env
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        settings_from(File::from_str(yaml, FileFormat::Yaml), &|key| {
            env.get(key).cloned()
        })
    }

    fn error_of(yaml: &str, env: &[(&str, &str)]) -> String {
        match parse_with_env(yaml, env) {
            Ok(_) => panic!("the config should be rejected"),
            Err(err) => format!("{err:#}"),
        }
    }

    fn upstreams(entries: &[(&str, &str)]) -> String {
        let mut yaml = String::from("upstreams:\n");
        for (label, url) in entries {
            yaml.push_str(&format!("  - label: {label}\n    url: {url}\n"));
        }
        yaml.push_str("application:\n  port: 8080\n");
        yaml
    }

    #[test]
    fn optional_fields_fall_back_to_their_defaults() {
        let settings = parse(MINIMAL).expect("the minimal config should load");

        assert_eq!(settings.application.host, "127.0.0.1");
        assert_eq!(settings.application.proxy.max_attempt, 3);
        assert_eq!(settings.application.proxy.rpc_timeout_in_secs, 3);
        assert_eq!(settings.application.proxy.retry_after_in_secs, 1);
        assert_eq!(settings.decider, DeciderKind::RoundRobin);
    }

    #[test]
    fn the_timeout_is_read_from_the_proxy_block() {
        let yaml = format!("{MINIMAL}  proxy:\n    rpc_timeout_in_secs: 9\n");
        let settings = parse(&yaml).expect("the config should load");

        assert_eq!(settings.application.proxy.rpc_timeout_in_secs, 9);
        assert_eq!(settings.legacy_rpc_timeout_in_secs, None);
    }

    #[test]
    fn the_legacy_top_level_timeout_still_wins() {
        let yaml = format!("{MINIMAL}rpc_timeout_in_secs: 9\n");
        let settings = parse(&yaml).expect("the config should load");

        assert_eq!(settings.application.proxy.rpc_timeout_in_secs, 9);
    }

    #[test]
    fn the_legacy_upstream_keys_still_load() {
        let yaml = "application:\n  port: 8080\nrpcs:\n  - label: one\n    rpc_url: http://127.0.0.1:9001\n";
        let settings = parse(yaml).expect("the config should load");

        assert_eq!(settings.upstreams[0].label, "one");
        assert_eq!(settings.upstreams[0].url, "http://127.0.0.1:9001");
    }

    #[test]
    fn the_decider_is_read_by_its_configured_spelling() {
        let yaml = format!("{MINIMAL}decider: PREFER_LEAST_ERRORS\n");
        let settings = parse(&yaml).expect("the config should load");

        assert_eq!(settings.decider, DeciderKind::PreferLeastErrors);
    }

    /// `config` writes its own message here and does not list the valid variants,
    /// so the assertion is on the key and the offending value.
    #[test]
    fn an_unrecognised_decider_is_rejected_by_name() {
        let error = error_of(&format!("{MINIMAL}decider: LEAST_LATENCY\n"), &[]);

        assert!(error.contains("LEAST_LATENCY"), "{error}");
        assert!(error.contains("decider"), "{error}");
    }

    /// The default must not be sticky: a container has to be able to widen it.
    #[test]
    fn an_explicit_host_overrides_the_loopback_default() {
        let yaml = format!("{MINIMAL}  host: 0.0.0.0\n");
        let settings = parse(&yaml).expect("the config should load");

        assert_eq!(settings.application.host, "0.0.0.0");
    }

    #[test]
    fn a_missing_port_is_an_error() {
        let yaml = "upstreams:\n  - label: one\n    url: http://127.0.0.1:9001\n";

        assert!(parse(yaml).is_err(), "application.port has no default");
    }

    #[test]
    fn missing_upstreams_are_an_error() {
        assert!(
            parse("application:\n  port: 8080\n").is_err(),
            "upstreams has no default"
        );
    }

    #[test]
    fn an_empty_upstream_list_is_an_error() {
        let error = error_of("application:\n  port: 8080\nupstreams: []\n", &[]);

        assert!(error.contains("upstreams must not be empty"), "{error}");
    }

    #[test]
    fn a_placeholder_is_expanded_from_the_environment() {
        let yaml = upstreams(&[("one", "https://provider.example/v2/${ALCHEMY_KEY}")]);
        let settings =
            parse_with_env(&yaml, &[("ALCHEMY_KEY", "secret")]).expect("the config should load");

        assert_eq!(
            settings.upstreams[0].url,
            "https://provider.example/v2/secret"
        );
    }

    #[test]
    fn every_string_field_is_expanded() {
        let yaml = format!(
            "{}  host: ${{HOST}}\n",
            upstreams(&[("${NAME}", "https://provider.example/${A}/${B}")])
        );
        let settings = parse_with_env(
            &yaml,
            &[
                ("HOST", "0.0.0.0"),
                ("NAME", "alchemy"),
                ("A", "v2"),
                ("B", "key"),
            ],
        )
        .expect("the config should load");

        assert_eq!(settings.application.host, "0.0.0.0");
        assert_eq!(settings.upstreams[0].label, "alchemy");
        assert_eq!(settings.upstreams[0].url, "https://provider.example/v2/key");
    }

    #[test]
    fn a_bare_dollar_variable_is_left_alone() {
        let yaml = upstreams(&[("one", "https://provider.example/$ALCHEMY_KEY")]);
        let settings =
            parse_with_env(&yaml, &[("ALCHEMY_KEY", "secret")]).expect("the config should load");

        assert_eq!(
            settings.upstreams[0].url,
            "https://provider.example/$ALCHEMY_KEY"
        );
    }

    #[test]
    fn an_unset_variable_fails_startup_and_names_the_variable() {
        let yaml = upstreams(&[("one", "https://provider.example/v2/${ALCHEMY_KEY}")]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("ALCHEMY_KEY"), "{error}");
        assert!(error.contains("unset"), "{error}");
    }

    #[test]
    fn an_empty_variable_counts_as_unset() {
        let yaml = upstreams(&[("one", "https://provider.example/v2/${ALCHEMY_KEY}")]);
        let error = error_of(&yaml, &[("ALCHEMY_KEY", "")]);

        assert!(error.contains("ALCHEMY_KEY"), "{error}");
    }

    #[test]
    fn an_expansion_failure_never_reveals_the_url() {
        let yaml = upstreams(&[(
            "alchemy",
            "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        )]);
        let error = error_of(&yaml, &[]);

        assert!(!error.contains("alchemy.com"), "{error}");
        assert!(!error.contains("eth-mainnet"), "{error}");
    }

    #[test]
    fn an_unterminated_placeholder_is_rejected_by_label() {
        let yaml = upstreams(&[("alchemy", "https://provider.example/v2/${ALCHEMY_KEY")]);
        let error = error_of(&yaml, &[("ALCHEMY_KEY", "secret")]);

        assert!(error.contains("alchemy"), "{error}");
        assert!(error.contains("placeholder"), "{error}");
        assert!(!error.contains("provider.example"), "{error}");
    }

    #[test]
    fn a_duplicate_label_is_an_error() {
        let yaml = upstreams(&[
            ("one", "https://provider.example/a"),
            ("one", "https://provider.example/b"),
        ]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("duplicate label one"), "{error}");
    }

    /// The two labels identify the offending entries; the shared URL holds the key.
    #[test]
    fn a_duplicate_url_names_both_labels_and_neither_url() {
        let yaml = upstreams(&[
            ("alchemy", "https://eth-mainnet.g.alchemy.com/v2/key"),
            ("alchemy-2", "https://eth-mainnet.g.alchemy.com/v2/key"),
        ]);
        let error = error_of(&yaml, &[]);

        assert!(
            error.contains("alchemy") && error.contains("alchemy-2"),
            "{error}"
        );
        assert!(!error.contains("alchemy.com"), "{error}");
    }

    #[test]
    fn an_empty_label_is_an_error() {
        let yaml = upstreams(&[("\"\"", "https://provider.example/a")]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("empty label"), "{error}");
    }

    #[test]
    fn an_empty_url_is_an_error() {
        let yaml = upstreams(&[("one", "\"\"")]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("empty url"), "{error}");
    }
}
