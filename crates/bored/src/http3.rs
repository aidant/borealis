use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use h3::ext::Protocol;
use http::Method;
use quinn::{TransportConfig, crypto::rustls::QuicServerConfig};
use rustls::server::ResolvesServerCert;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info};

pub async fn start_server(
    addr: SocketAddr,
    cert_resolver: Arc<dyn ResolvesServerCert>,
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
            let future = accept_quinn_connection(conn);

            tokio::spawn(async move {
                if let Err(e) = future.await {
                    error!("HTTP/3 connection failed: {:?}", e);
                }
            });
        }
    }
}

async fn accept_quinn_connection(conn: quinn::Incoming) -> Result<(), anyhow::Error> {
    debug!("incomming HTTP/3 connection");

    let conn = h3::server::builder()
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1)
        .send_grease(true)
        .build::<h3_quinn::Connection, Bytes>(h3_quinn::Connection::new(conn.await?))
        .await?;

    accept_h3_connection(conn).await;

    Ok(())
}

async fn accept_h3_connection(mut conn: h3::server::Connection<h3_quinn::Connection, Bytes>) -> () {
    loop {
        let Ok(Some(resolver)) = conn
            .accept()
            .await
            .inspect_err(|e| error!("HTTP/3 connection failed: {:?}", e))
        else {
            break;
        };

        let Ok((req, mut stream)) = resolver
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

        // TODO: http server

        let response = http::Response::builder().status(200).body(()).unwrap();

        match stream.send_response(response).await {
            Ok(_) => info!("sent 200 ok"),
            Err(e) => error!("unable to send response: {:?}", e),
        };

        let payload = Bytes::from("<script>wt = new WebTransport(location.origin)</script>");

        match stream.send_data(payload).await {
            Ok(_) => info!("sent payload"),
            Err(e) => error!("unable to send payload: {:?}", e),
        };

        match stream.finish().await {
            Ok(_) => info!("finished"),
            Err(e) => error!("unable to finish: {:?}", e),
        };
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
