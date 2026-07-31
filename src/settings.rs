use anyhow::{Result, anyhow};
use config::Config;

#[derive(serde::Deserialize)]
pub struct Settings {
    pub rpcs: Vec<RpcSettings>,
    pub retry_count: Option<u64>,
    pub rpc_timeout: Option<u64>,
    pub retry_after: Option<u64>,
}

#[derive(serde::Deserialize)]
pub struct RpcSettings {
    pub label: String,
    pub rpc_url: String,
}

pub fn get_settings() -> Result<Settings> {
    Config::builder()
        .add_source(config::File::with_name("settings.yaml"))
        .build()?
        .try_deserialize::<Settings>()
        .map_err(|e| anyhow!("{e}"))
}
