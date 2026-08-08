use anyhow::{Context, Result};
use config::{Config, Source};

const CONFIG_FILE: &str = "settings.yaml";

#[derive(serde::Deserialize)]
pub struct Settings {
    pub rpcs: Vec<RpcSettings>,
    pub application_host: String,
    pub application_port: u16,
    pub max_attempt: u64,
    pub rpc_timeout_in_secs: u64,
    pub retry_after_in_secs: u64,
}

#[derive(serde::Deserialize)]
pub struct RpcSettings {
    pub label: String,
    pub rpc_url: String,
}

pub fn get_settings() -> Result<Settings> {
    settings_from(config::File::with_name(CONFIG_FILE))
}

/// Takes the source rather than naming the file, so the defaults below are
/// reachable from a test without a `settings.yaml` on disk — it is gitignored,
/// so no test could rely on one being there.
fn settings_from<S>(source: S) -> Result<Settings>
where
    S: Source + Send + Sync + 'static,
{
    Config::builder()
        // Loopback: widening the bind is a deliberate act, because there is no
        // auth yet and a reachable proxy spends the upstream API keys.
        .set_default("application_host", "127.0.0.1")?
        .set_default("max_attempt", 3)?
        .set_default("rpc_timeout_in_secs", 3)?
        .set_default("retry_after_in_secs", 1)?
        .add_source(source)
        .build()?
        .try_deserialize::<Settings>()
        .context("invalid settings")
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{File, FileFormat};

    /// Only the two required keys, so every optional falls through to its default.
    const MINIMAL: &str = r#"
application_port: 8080
rpcs:
  - label: one
    rpc_url: http://127.0.0.1:9001
"#;

    fn parse(yaml: &str) -> Result<Settings> {
        settings_from(File::from_str(yaml, FileFormat::Yaml))
    }

    #[test]
    fn optional_fields_fall_back_to_their_defaults() {
        let settings = parse(MINIMAL).expect("the minimal config should load");

        assert_eq!(settings.application_host, "127.0.0.1");
        assert_eq!(settings.max_attempt, 3);
        assert_eq!(settings.rpc_timeout_in_secs, 3);
        assert_eq!(settings.retry_after_in_secs, 1);
    }

    /// The default must not be sticky: a container has to be able to widen it.
    #[test]
    fn an_explicit_host_overrides_the_loopback_default() {
        let yaml = format!("{MINIMAL}application_host: 0.0.0.0\n");
        let settings = parse(&yaml).expect("the config should load");

        assert_eq!(settings.application_host, "0.0.0.0");
    }

    #[test]
    fn a_missing_port_is_an_error() {
        let yaml = "rpcs:\n  - label: one\n    rpc_url: http://127.0.0.1:9001\n";

        assert!(parse(yaml).is_err(), "application_port has no default");
    }

    #[test]
    fn missing_upstreams_are_an_error() {
        assert!(
            parse("application_port: 8080\n").is_err(),
            "rpcs has no default"
        );
    }
}
