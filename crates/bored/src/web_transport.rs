use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::app::ServerName;

#[derive(Debug)]
pub struct WebTransportStreamProvider {
    rx: mpsc::Receiver<
        oneshot::Sender<
            Result<
                h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                anyhow::Error,
            >,
        >,
    >,
}

pub struct WebTransportStreamSender {
    tx: oneshot::Sender<
        Result<
            h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
            anyhow::Error,
        >,
    >,
}

#[derive(Debug, Clone)]
pub struct WebTransportStreamReceiver {
    tx: mpsc::Sender<
        oneshot::Sender<
            Result<
                h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                anyhow::Error,
            >,
        >,
    >,
}

impl WebTransportStreamProvider {
    pub async fn get_sender(&mut self) -> Option<WebTransportStreamSender> {
        self.rx
            .recv()
            .await
            .map(|tx| WebTransportStreamSender { tx })
    }
}

impl WebTransportStreamSender {
    pub fn send<E: std::error::Error + Send + Sync + 'static>(
        self,
        result: Result<h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>, E>,
    ) {
        match result {
            Ok(stream) => {
                let _ = self.tx.send(Ok(stream)).inspect_err(|_| {
                    error!("error sending bidirectional WebTransport stream");
                });
            }
            Err(e) => {
                let _ = self.tx.send(Err(e.into())).inspect_err(|_| {
                    error!("error sending bidirectional WebTransport error");
                });
            }
        }
    }
}

impl WebTransportStreamReceiver {
    pub async fn request_web_transport_stream(
        &self,
    ) -> Result<
        h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
        anyhow::Error,
    > {
        let (resp_tx, resp_rx) = oneshot::channel();

        if self.tx.send(resp_tx).await.is_err() {
            return Err(anyhow::anyhow!("WebTransport session task dropped"));
        }

        let web_transport_stream = resp_rx.await??;

        Ok(web_transport_stream)
    }
}

#[derive(Debug, Clone)]
pub struct WebTransportRegistry {
    requests: Arc<
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

    pub fn insert(&self, server_name: &ServerName) -> WebTransportStreamProvider {
        let (tx, rx) = mpsc::channel::<
            oneshot::Sender<
                Result<
                    h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                    anyhow::Error,
                >,
            >,
        >(32);

        self.requests.insert(server_name.to_owned(), tx);

        WebTransportStreamProvider { rx }
    }

    pub fn get(&self, server_name: &ServerName) -> Option<WebTransportStreamReceiver> {
        self.requests
            .get(server_name)
            .map(|tx| WebTransportStreamReceiver { tx: tx.to_owned() })
    }

    pub fn remove(&self, server_name: &ServerName) {
        self.requests.remove(server_name);
    }
}
