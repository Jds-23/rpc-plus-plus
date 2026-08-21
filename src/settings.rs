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
    pub rpcs: Vec<RpcSettings>,
    pub rpc_timeout_in_secs: u64,
    pub decider: String,
}

#[derive(serde::Deserialize)]
pub struct RpcSettings {
    pub label: String,
    pub rpc_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    #[error("environment variable {0} is unset")]
    UnsetEnvVar(String),
    #[error("upstream {label} still holds an unexpanded ${{...}} placeholder")]
    ResidualPlaceholder { label: String },
    #[error("rpcs must not be empty")]
    EmptyRpcs,
    #[error("rpcs[{index}] has an empty label")]
    EmptyLabel { index: usize },
    #[error("upstream {label} has an empty rpc_url")]
    EmptyUrl { label: String },
    #[error("duplicate label {0}")]
    DuplicateLabel(String),
    #[error("upstreams {first} and {second} share an rpc_url")]
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
        .set_default("rpc_timeout_in_secs", 3)?
        .set_default("decider", "ROUND_ROBIN")?
        .add_source(source)
        .build()?
        .try_deserialize::<Settings>()
        .context("invalid settings")?;

    expand_env(&mut settings, get).context("invalid settings")?;
    validate_settings(&settings).context("invalid settings")?;

    Ok(settings)
}

fn expand_env(
    settings: &mut Settings,
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<(), SettingsError> {
    settings.application.host = expand_str(&settings.application.host, get)?;

    for rpc in &mut settings.rpcs {
        rpc.label = expand_str(&rpc.label, get)?;
        rpc.rpc_url = expand_str(&rpc.rpc_url, get)?;
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
    if settings.rpcs.is_empty() {
        return Err(SettingsError::EmptyRpcs);
    }

    let mut labels: HashSet<&str> = HashSet::new();
    let mut urls: HashMap<&str, &str> = HashMap::new();

    for (index, rpc) in settings.rpcs.iter().enumerate() {
        if rpc.label.trim().is_empty() {
            return Err(SettingsError::EmptyLabel { index });
        }
        if rpc.rpc_url.trim().is_empty() {
            return Err(SettingsError::EmptyUrl {
                label: rpc.label.clone(),
            });
        }
        if rpc.label.contains("${") || rpc.rpc_url.contains("${") {
            return Err(SettingsError::ResidualPlaceholder {
                label: rpc.label.clone(),
            });
        }
        if !labels.insert(rpc.label.as_str()) {
            return Err(SettingsError::DuplicateLabel(rpc.label.clone()));
        }
        if let Some(first) = urls.insert(rpc.rpc_url.as_str(), rpc.label.as_str()) {
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
rpcs:
  - label: one
    rpc_url: http://127.0.0.1:9001
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

    fn rpcs(entries: &[(&str, &str)]) -> String {
        let mut yaml = String::from("rpcs:\n");
        for (label, url) in entries {
            yaml.push_str(&format!("  - label: {label}\n    rpc_url: {url}\n"));
        }
        yaml.push_str("application:\n  port: 8080\n");
        yaml
    }

    #[test]
    fn optional_fields_fall_back_to_their_defaults() {
        let settings = parse(MINIMAL).expect("the minimal config should load");

        assert_eq!(settings.application.host, "127.0.0.1");
        assert_eq!(settings.application.proxy.max_attempt, 3);
        assert_eq!(settings.rpc_timeout_in_secs, 3);
        assert_eq!(settings.application.proxy.retry_after_in_secs, 1);
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
        let yaml = "rpcs:\n  - label: one\n    rpc_url: http://127.0.0.1:9001\n";

        assert!(parse(yaml).is_err(), "application.port has no default");
    }

    #[test]
    fn missing_upstreams_are_an_error() {
        assert!(
            parse("application:\n  port: 8080\n").is_err(),
            "rpcs has no default"
        );
    }

    #[test]
    fn an_empty_upstream_list_is_an_error() {
        let error = error_of("application:\n  port: 8080\nrpcs: []\n", &[]);

        assert!(error.contains("rpcs must not be empty"), "{error}");
    }

    #[test]
    fn a_placeholder_is_expanded_from_the_environment() {
        let yaml = rpcs(&[("one", "https://provider.example/v2/${ALCHEMY_KEY}")]);
        let settings =
            parse_with_env(&yaml, &[("ALCHEMY_KEY", "secret")]).expect("the config should load");

        assert_eq!(
            settings.rpcs[0].rpc_url,
            "https://provider.example/v2/secret"
        );
    }

    #[test]
    fn every_string_field_is_expanded() {
        let yaml = format!(
            "{}  host: ${{HOST}}\n",
            rpcs(&[("${NAME}", "https://provider.example/${A}/${B}")])
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
        assert_eq!(settings.rpcs[0].label, "alchemy");
        assert_eq!(settings.rpcs[0].rpc_url, "https://provider.example/v2/key");
    }

    #[test]
    fn a_bare_dollar_variable_is_left_alone() {
        let yaml = rpcs(&[("one", "https://provider.example/$ALCHEMY_KEY")]);
        let settings =
            parse_with_env(&yaml, &[("ALCHEMY_KEY", "secret")]).expect("the config should load");

        assert_eq!(
            settings.rpcs[0].rpc_url,
            "https://provider.example/$ALCHEMY_KEY"
        );
    }

    #[test]
    fn an_unset_variable_fails_startup_and_names_the_variable() {
        let yaml = rpcs(&[("one", "https://provider.example/v2/${ALCHEMY_KEY}")]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("ALCHEMY_KEY"), "{error}");
        assert!(error.contains("unset"), "{error}");
    }

    #[test]
    fn an_empty_variable_counts_as_unset() {
        let yaml = rpcs(&[("one", "https://provider.example/v2/${ALCHEMY_KEY}")]);
        let error = error_of(&yaml, &[("ALCHEMY_KEY", "")]);

        assert!(error.contains("ALCHEMY_KEY"), "{error}");
    }

    #[test]
    fn an_expansion_failure_never_reveals_the_url() {
        let yaml = rpcs(&[(
            "alchemy",
            "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        )]);
        let error = error_of(&yaml, &[]);

        assert!(!error.contains("alchemy.com"), "{error}");
        assert!(!error.contains("eth-mainnet"), "{error}");
    }

    #[test]
    fn an_unterminated_placeholder_is_rejected_by_label() {
        let yaml = rpcs(&[("alchemy", "https://provider.example/v2/${ALCHEMY_KEY")]);
        let error = error_of(&yaml, &[("ALCHEMY_KEY", "secret")]);

        assert!(error.contains("alchemy"), "{error}");
        assert!(error.contains("placeholder"), "{error}");
        assert!(!error.contains("provider.example"), "{error}");
    }

    #[test]
    fn a_duplicate_label_is_an_error() {
        let yaml = rpcs(&[
            ("one", "https://provider.example/a"),
            ("one", "https://provider.example/b"),
        ]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("duplicate label one"), "{error}");
    }

    /// The two labels identify the offending entries; the shared URL holds the key.
    #[test]
    fn a_duplicate_url_names_both_labels_and_neither_url() {
        let yaml = rpcs(&[
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
        let yaml = rpcs(&[("\"\"", "https://provider.example/a")]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("empty label"), "{error}");
    }

    #[test]
    fn an_empty_url_is_an_error() {
        let yaml = rpcs(&[("one", "\"\"")]);
        let error = error_of(&yaml, &[]);

        assert!(error.contains("empty rpc_url"), "{error}");
    }
}
