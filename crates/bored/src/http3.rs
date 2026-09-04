use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use axum::Router;
use bytes::Bytes;
use h3::ext::Protocol;
use http::{Method, Request};
use hyper_util::service::TowerToHyperService;
use quinn::{TransportConfig, crypto::rustls::QuicServerConfig};
use rustls::server::ResolvesServerCert;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info};

use crate::app::Server;

pub async fn start_server(
    addr: SocketAddr,
    cert_resolver: Arc<dyn ResolvesServerCert>,
    app: Router,
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
            let future = accept_quinn_connection(conn, app.clone());

            tokio::spawn(async move {
                if let Err(e) = future.await {
                    error!("HTTP/3 connection failed: {:?}", e);
                }
            });
        }
    }
}

async fn accept_quinn_connection(conn: quinn::Incoming, app: Router) -> Result<(), anyhow::Error> {
    debug!("incomming HTTP/3 connection");

    let conn = h3::server::builder()
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1)
        .send_grease(true)
        .build::<h3_quinn::Connection, Bytes>(h3_quinn::Connection::new(conn.await?))
        .await?;

    accept_h3_connection(conn, app).await;

    Ok(())
}

async fn accept_h3_connection(
    mut conn: h3::server::Connection<h3_quinn::Connection, Bytes>,
    app: Router,
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

        let method = req.method();
        let ext = req.extensions();

        if method == &Method::CONNECT && ext.get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT) {
            let Ok(session) =
                h3_webtransport::server::WebTransportSession::accept(req, stream, conn).await
            else {
                break;
            };

            accept_h3_session(session).await;

            break;
        }

        let _ = accept_h3_request(req, stream, app.clone())
            .await
            .inspect_err(|e| error!("HTTP/3 request failed: {:?}", e));
    }

    ()
}

async fn accept_h3_session(
    session: h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>,
) -> () {
    loop {
        match session.accept_bi().await {
            Ok(Some(h3_webtransport::server::AcceptedBi::BidiStream(_session_id, mut stream))) => {
                let mut buf = vec![0; 1024];
                while let Ok(n) = stream.read(&mut buf).await {
                    stream.write_all(&buf[..n]).await.ok();
                }
            }
            Ok(Some(h3_webtransport::server::AcceptedBi::Request(_req, mut _stream))) => {
                todo!()
            }
            Ok(None) => continue,
            Err(e) => {
                error!(
                    "error accepting HTTP/3 WebTransport bidirectional stream {:?}",
                    e
                );
                continue;
            }
        }
    }
}

async fn accept_h3_request(
    req: Request<()>,
    stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    app: Router,
) -> Result<(), anyhow::Error> {
    let hyper_service = TowerToHyperService::new(tower::ServiceBuilder::new().service(app));

    let (mut parts, _body) = req.into_parts();

    parts.extensions.insert(Server::Http3);

    let (mut send, recv) = stream.split();

    let req = hyper::Request::from_parts(parts, h3_util::server_body::H3IncomingServer::new(recv));
    let res = hyper::service::Service::call(&hyper_service, req).await?;

    let (res_parts, res_body) = res.into_parts();
    send.send_response(hyper::Response::from_parts(res_parts, ()))
        .await?;

    let mut body_stream = std::pin::pin!(res_body);

    while let Some(frame) = http_body_util::BodyExt::frame(&mut body_stream).await {
        if let Some(chunk) = frame?.data_ref() {
            if !chunk.is_empty() {
                send.send_data(chunk.to_owned().into()).await?;
            }
        }
    }

    send.finish().await?;

    Ok(())
}
