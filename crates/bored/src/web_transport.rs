use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use crate::app::ServerName;

#[derive(Debug, Clone)]
pub struct WebTransportRegistry {
    pub requests: Arc<
        DashMap<
            ServerName,
            mpsc::Sender<
                oneshot::Sender<
                    Result<
                        h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                        anyhow::Error,
                    >,
                >,
            >,
        >,
    >,
}

impl WebTransportRegistry {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(DashMap::new()),
        }
    }
}
