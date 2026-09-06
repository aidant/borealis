use bytes::Bytes;
use http::StatusCode;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    service::TowerToHyperService,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_rustls::server::TlsStream;
use tracing::error;

use crate::http_util::{
    http1_response_parts_to_response_parts, request_parts_to_http1_request_parts,
};

type ProxiedResponse = hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>>;

pub async fn h2_to_http1(
    h2_stream: TlsStream<TcpStream>,
    tx: mpsc::Sender<
        oneshot::Sender<
            Result<
                h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                anyhow::Error,
            >,
        >,
    >,
) -> Result<(), anyhow::Error> {
    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection_with_upgrades(
            TokioIo::new(h2_stream),
            TowerToHyperService::new(tower::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let tx = tx.clone();

                    async move { proxy_request_to_web_transport(request, tx).await }
                },
            )),
        )
        .await
        .map_err(anyhow::Error::from_boxed)?;

    Ok(())
}

async fn proxy_request_to_web_transport(
    request: hyper::Request<hyper::body::Incoming>,
    tx: mpsc::Sender<
        oneshot::Sender<
            Result<
                h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                anyhow::Error,
            >,
        >,
    >,
) -> Result<ProxiedResponse, anyhow::Error> {
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<
        Result<
            h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
            anyhow::Error,
        >,
    >();

    if tx.send(resp_tx).await.is_err() {
        return Err(anyhow::anyhow!("WebTransport session task dropped"));
    }

    let web_transport_stream = resp_rx.await??;

    let (parts, body) = request.into_parts();

    let parts = match request_parts_to_http1_request_parts(parts, body.size_hint().exact()) {
        Ok(parts) => parts,
        Err(status) => return Ok(build_error(status)),
    };

    let request = hyper::Request::from_parts(parts, body);

    let (mut sender, connection) =
        match hyper::client::conn::http1::handshake(TokioIo::new(web_transport_stream)).await {
            Ok(handshake) => handshake,
            Err(e) => {
                error!("HTTP/1.1 WebTransport handshake failed: {:?}", e);
                return Ok(build_error(StatusCode::BAD_GATEWAY));
            }
        };

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("HTTP/1.1 WebTransport connection failed: {:?}", e);
        }
    });

    let response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(e) => {
            error!("HTTP/1.1 WebTransport request failed: {:?}", e);
            return Ok(build_error(StatusCode::BAD_GATEWAY));
        }
    };

    let (parts, body) = response.into_parts();

    let parts = match http1_response_parts_to_response_parts(parts) {
        Ok(parts) => parts,
        Err(status) => return Ok(build_error(status)),
    };

    Ok(hyper::Response::from_parts(parts, body.boxed()))
}

fn build_error(status: StatusCode) -> ProxiedResponse {
    hyper::Response::builder()
        .status(status)
        .body(
            http_body_util::Full::<Bytes>::new(Bytes::new())
                .map_err(|error| match error {})
                .boxed(),
        )
        .unwrap()
}
