use crate::{
    rpc_handler::{RpcHandler, RpcHandlerBuilder},
    settings::RpcSettings,
};

pub fn build_handlers(rpcs: Vec<RpcSettings>) -> Vec<RpcHandler> {
    rpcs.into_iter()
        .filter_map(|item| {
            match RpcHandlerBuilder::default()
                .with_url(item.rpc_url.clone())
                .build()
            {
                Ok(item) => Some(item),
                Err(err) => {
                    tracing::warn!(
                        url = %&item.label,
                        error = format!("{err:#}"),
                        "skipping rpc backend"
                    );
                    None
                }
            }
        })
        .collect()
}
