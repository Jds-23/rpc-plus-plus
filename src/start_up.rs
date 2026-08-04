use crate::{
    rpc_handler::{RpcHandler, RpcHandlerBuilder},
    settings::RpcSettings,
};

pub fn build_handlers<I>(rpcs: I, rpc_timeout_in_secs: u64) -> Vec<RpcHandler>
where
    I: IntoIterator<Item = RpcSettings>,
{
    rpcs.into_iter()
        .filter_map(|item| {
            match RpcHandlerBuilder::default()
                .with_label(item.label.clone())
                .with_rpc_timeout_in_secs(rpc_timeout_in_secs)
                .with_url(item.rpc_url)
                .build()
            {
                Ok(item) => Some(item),
                Err(err) => {
                    tracing::warn!(
                        upstream = %&item.label,
                        error = format!("{err:#}"),
                        "skipping rpc backend"
                    );
                    None
                }
            }
        })
        .collect()
}
