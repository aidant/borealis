use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

use crate::{
    app::{ServerImpl, ServerName},
    http_util::{self, H3Body, ProxiedResponse, hyper_to_http1},
    web_transport::{WebTransportRegistry, WebTransportStreamReceiver, WebTransportStreamSender},
};
use axum::{
    Router,
    middleware::{self, Next},
};
use bytes::Bytes;
use h3::ext::Protocol;
use http::{Method, StatusCode, Uri};
use hyper_util::service::TowerToHyperService;
use quinn::{TransportConfig, crypto::rustls::QuicServerConfig};
use rustls::server::ResolvesServerCert;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info};

pub async fn start_server(
    addr: SocketAddr,
    cert_resolver: Arc<dyn ResolvesServerCert>,
    app: Router,
    registry: WebTransportRegistry,
) -> Result<(), io::Error> {
    let app = app.layer(middleware::from_fn(
        |mut req: axum::extract::Request, next: Next| async move {
            req.extensions_mut().insert(ServerImpl::Http3);

            next.run(req).await
        },
    ));

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
    let app = app.layer(middleware::from_fn({
        let server_name = server_name.clone();

        move |mut req: axum::extract::Request, next: Next| {
            let server_name = server_name.clone();

            async move {
                if let Some(server_name) = server_name {
                    req.extensions_mut().insert(server_name);
                };

                next.run(req).await
            }
        }
    }));

    let hyper_service = TowerToHyperService::new(tower::ServiceBuilder::new().service(app));

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
            if let Some(web_transport_stream_receiver) = registry.get(&server_name) {
                let future =
                    bind_h3_stream_to_web_transport(web_transport_stream_receiver, req, stream);

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

        let future = http_util::h3_server(req, stream, hyper_service.to_owned());

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

    let mut web_transport_stream_provider = match registry.insert(&server_name) {
        Ok(provider) => provider,
        Err(e) => {
            error!("error registering WebTransport session: {e:?}");
            return;
        }
    };

    let session_id = session.session_id();

    tokio::spawn(async move {
        async fn open_bi(
            web_transport_stream_sender: WebTransportStreamSender,
            session_id: h3_webtransport::SessionId,
            session: &h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>,
        ) -> bool {
            let result = session.open_bi(session_id).await;
            let is_err = result.is_err();
            web_transport_stream_sender.send(result);

            is_err
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
                Some(web_transport_stream_sender) = web_transport_stream_provider.get_sender() => {
                    if open_bi(web_transport_stream_sender, session_id, &session).await {
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

        registry.remove(&server_name);
    });
}

async fn bind_h3_stream_to_web_transport(
    web_transport_stream_receiver: WebTransportStreamReceiver,
    h3_request: http::Request<()>,
    h3_stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
) -> Result<(), anyhow::Error> {
    let http1_service = TowerToHyperService::new(tower::service_fn(
        move |request: hyper::Request<H3Body<h3_quinn::RecvStream>>| {
            let web_transport_stream_receiver = web_transport_stream_receiver.clone();

            async move { bind_request_to_web_transport(web_transport_stream_receiver, request).await }
        },
    ));

    http_util::h3_server(h3_request, h3_stream, http1_service).await
}

pub async fn bind_request_to_web_transport<B>(
    web_transport_stream_receiver: WebTransportStreamReceiver,
    request: hyper::Request<B>,
) -> Result<ProxiedResponse, anyhow::Error>
where
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let web_transport_stream = web_transport_stream_receiver
        .request_web_transport_stream()
        .await?;

    Ok(hyper_to_http1(request, web_transport_stream).await)
}
