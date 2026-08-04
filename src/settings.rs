use anyhow::{Context, Result};
use config::Config;

#[derive(serde::Deserialize)]
pub struct Settings {
    pub rpcs: Vec<RpcSettings>,
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
    Config::builder()
        .set_default("max_attempt", 3)?
        .set_default("rpc_timeout_in_secs", 3)?
        .set_default("retry_after_in_secs", 1)?
        .add_source(config::File::with_name("settings.yaml"))
        .build()?
        .try_deserialize::<Settings>()
        .context("invalid settings")
}
