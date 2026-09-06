use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

use crate::{
    app::{ServerImpl, ServerName},
    http_util::{self, h3_recv_body, h3_send_body},
    web_transport::WebTransportRegistry,
};
use axum::Router;
use bytes::Bytes;
use h3::{ext::Protocol, server::RequestStream};
use http::{Method, Request, StatusCode, Uri};
use hyper_util::service::TowerToHyperService;
use quinn::{TransportConfig, crypto::rustls::QuicServerConfig};
use rustls::server::ResolvesServerCert;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};
use tracing::{debug, error, info};

pub async fn start_server(
    addr: SocketAddr,
    cert_resolver: Arc<dyn ResolvesServerCert>,
    app: Router,
    registry: WebTransportRegistry,
) -> Result<(), io::Error> {
    let mut server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_cert_resolver(cert_resolver);
    server_config.max_early_data_size = u32::MAX;
    server_config.alpn_protocols = vec![b"h3".to_vec()];

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(server_config).unwrap(),
    ));

    if let Some(transport_config) = Arc::get_mut(&mut server_config.transport) {
        transport_config.keep_alive_interval(Some(Duration::from_secs(2)));
    } else {
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Some(Duration::from_secs(2)));
        server_config.transport = Arc::new(transport_config);
    }

    loop {
        let endpoint = quinn::Endpoint::server(server_config.clone(), addr)?;

        info!("HTTP/3 server listening on: {}", addr);

        while let Some(conn) = endpoint.accept().await {
            let future = accept_quinn_connection(conn, app.clone(), registry.clone());

            tokio::spawn(async move {
                if let Err(e) = future.await {
                    error!("HTTP/3 connection failed: {:?}", e);
                }
            });
        }
    }
}

async fn accept_quinn_connection(
    conn: quinn::Incoming,
    app: Router,
    registry: WebTransportRegistry,
) -> Result<(), anyhow::Error> {
    debug!("incomming HTTP/3 connection");

    let conn = conn.await?;

    let server_name = conn
        .handshake_data()
        .and_then(|handshake_data| {
            handshake_data
                .downcast_ref::<quinn::crypto::rustls::HandshakeData>()
                .and_then(|data| data.server_name.clone())
        })
        .map(|server_name| ServerName(server_name));

    let conn = h3::server::builder()
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1)
        .send_grease(true)
        .build::<h3_quinn::Connection, Bytes>(h3_quinn::Connection::new(conn))
        .await?;

    accept_h3_connection(conn, app, server_name, registry).await;

    Ok(())
}

async fn accept_h3_connection(
    mut conn: h3::server::Connection<h3_quinn::Connection, Bytes>,
    app: Router,
    server_name: Option<ServerName>,
    registry: WebTransportRegistry,
) -> () {
    loop {
        let Ok(Some(resolver)) = conn
            .accept()
            .await
            .inspect_err(|e| error!("HTTP/3 connection failed: {:?}", e))
        else {
            break;
        };

        let Ok((req, stream)) = resolver
            .resolve_request()
            .await
            .inspect_err(|e| error!("HTTP/3 request failed: {:?}", e))
        else {
            continue;
        };

        if let Some(server_name) = server_name.clone() {
            if let Some(tx) = registry.requests.get(&server_name) {
                let future = bind_h3_request_to_web_transport(tx.to_owned(), req, stream);

                tokio::spawn(async move {
                    if let Err(e) = future.await {
                        error!("HTTP/3 request failed: {:?}", e);
                    }
                });

                continue;
            };
        };

        let method = req.method();
        let uri = req.uri().to_owned();
        let ext = req.extensions();

        if method == &Method::CONNECT && ext.get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT) {
            let Ok(session) =
                h3_webtransport::server::WebTransportSession::accept(req, stream, conn).await
            else {
                break;
            };

            tokio::spawn(async move {
                accept_h3_session(session, uri, server_name, registry).await;
            });

            break;
        }

        let future = accept_h3_request(req, stream, app.clone(), server_name.clone());

        tokio::spawn(async move {
            if let Err(e) = future.await {
                error!("HTTP/3 request failed: {:?}", e);
            }
        });
    }

    ()
}

async fn accept_h3_session(
    session: h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>,
    uri: Uri,
    server_name: Option<ServerName>,
    registry: WebTransportRegistry,
) -> () {
    let query_params = uri.query().and_then(|query| {
        Some(
            url::form_urlencoded::parse(query.as_bytes())
                .into_iter()
                .collect::<HashMap<_, _>>(),
        )
    });

    let Some(server_name) = query_params
        .and_then(|query_params| {
            query_params
                .get("host")
                .and_then(|host| Some(ServerName(host.to_string())))
        })
        .or_else(|| server_name)
    else {
        return;
    };

    let (tx, mut rx) = mpsc::channel::<
        oneshot::Sender<
            Result<
                h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                anyhow::Error,
            >,
        >,
    >(32);

    registry.requests.insert(server_name.clone(), tx);

    let session_id = session.session_id();

    tokio::spawn(async move {
        async fn open_bi(
            tx: oneshot::Sender<
                Result<
                    h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                    anyhow::Error,
                >,
            >,
            session_id: h3_webtransport::SessionId,
            session: &h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>,
        ) -> bool {
            match session.open_bi(session_id).await {
                Ok(stream) => {
                    let _ = tx.send(Ok(stream)).inspect_err(|_| {
                        error!("error sending bidirectional WebTransport stream");
                    });
                }
                Err(e) => {
                    let _ = tx.send(Err(e.into())).inspect_err(|_| {
                        error!("error sending bidirectional WebTransport error");
                    });

                    return true;
                }
            }

            false
        }

        async fn accept_bi(
            session: &h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>,
        ) -> bool {
            match session.accept_bi().await {
                Ok(Some(h3_webtransport::server::AcceptedBi::BidiStream(
                    _session_id,
                    mut stream,
                ))) => {
                    let mut buf = vec![0; 1024];
                    while let Ok(n) = stream.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        if stream.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(Some(h3_webtransport::server::AcceptedBi::Request(_req, mut stream))) => {
                    let response = http::Response::builder()
                        .status(StatusCode::NOT_IMPLEMENTED)
                        .body(())
                        .unwrap();

                    let _ = stream.send_response(response).await;
                    let _ = stream.finish().await;
                }
                Ok(None) => {}
                Err(e) => {
                    error!(
                        "error accepting HTTP/3 WebTransport bidirectional stream {:?}",
                        e
                    );
                    return true;
                }
            };

            return false;
        }

        loop {
            tokio::select! {
                Some(tx) = rx.recv() => {
                    if open_bi(tx, session_id, &session).await {
                        break;
                    }
                }
                res = accept_bi(&session) => {
                    if res {
                        break;
                    }
                }
            }
        }

        registry.requests.remove(&server_name);
    });
}

async fn accept_h3_request<S>(
    h3_request: Request<()>,
    h3_stream: RequestStream<S, Bytes>,
    app: Router,
    server_name: Option<ServerName>,
) -> Result<(), anyhow::Error>
where
    S: h3::quic::BidiStream<Bytes> + Send,
    <S as h3::quic::BidiStream<bytes::Bytes>>::RecvStream: Send + 'static,
{
    let hyper_service = TowerToHyperService::new(tower::ServiceBuilder::new().service(app));

    let (mut h3_request_parts, _) = h3_request.into_parts();

    h3_request_parts.extensions.insert(ServerImpl::Http3);
    if let Some(server_name) = server_name {
        h3_request_parts.extensions.insert(server_name);
    }

    let (mut h3_send, h3_recv) = h3_stream.split();

    let request = hyper::Request::from_parts(h3_request_parts, h3_recv_body(h3_recv));
    let response = hyper::service::Service::call(&hyper_service, request).await?;

    let (response_parts, response_body) = response.into_parts();

    h3_send
        .send_response(hyper::Response::from_parts(response_parts, ()))
        .await?;

    h3_send_body::<S, _>(&mut h3_send, response_body).await?;

    Ok(())
}

async fn bind_h3_request_to_web_transport(
    tx: mpsc::Sender<
        oneshot::Sender<
            Result<
                h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                anyhow::Error,
            >,
        >,
    >,
    req: Request<()>,
    stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
) -> Result<(), anyhow::Error> {
    let (resp_tx, resp_rx) = oneshot::channel::<
        Result<
            h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
            anyhow::Error,
        >,
    >();

    if tx.send(resp_tx).await.is_err() {
        return Err(anyhow::anyhow!("WebTransport session task dropped"));
    }

    let web_transport_stream = resp_rx.await??;

    http_util::h3_to_http1(req, stream, web_transport_stream).await?;

    Ok(())
}
