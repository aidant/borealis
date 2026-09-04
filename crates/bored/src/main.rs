use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use h3::ext::Protocol;
use http::Method;
use quinn::crypto::rustls::QuicServerConfig;
use tracing::{error, info};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_span_events(FmtSpan::FULL)
            .with_env_filter(tracing_subscriber::EnvFilter::from_env("BOREALIS_LOG"))
            .finish(),
    )?;

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    start_server().await?;

    Ok(())
}

async fn start_server() -> Result<(), anyhow::Error> {
    let server_addr: SocketAddr = "127.0.0.1:8443".parse()?;

    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])?;

    let mut server_crypto =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                cert.signing_key.serialize_der().try_into().unwrap(),
            )?;

    server_crypto.max_early_data_size = u32::MAX;
    server_crypto.alpn_protocols = vec![b"h3".to_vec()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));

    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.keep_alive_interval(Some(Duration::from_secs(2)));

    let endpoint = quinn::Endpoint::server(server_config, server_addr)?;

    info!("listening on {}", server_addr);

    while let Some(conn) = endpoint.accept().await {
        info!("new connection");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(conn).await {
                error!("connection failed: {}", e.to_string());
            }
        });
    }

    Ok(())
}

async fn handle_connection(conn: quinn::Incoming) -> Result<(), anyhow::Error> {
    let conn = conn.await?;

    info!("new connection established");

    let mut conn = h3::server::builder()
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1)
        .send_grease(true)
        .build::<h3_quinn::Connection, Bytes>(h3_quinn::Connection::new(conn))
        .await?;

    loop {
        let resolver = match conn.accept().await {
            Ok(Some(resolver)) => resolver,
            Ok(None) => break,
            Err(e) => {
                error!("connection failed: {}", e.to_string());
                break;
            }
        };

        let (req, mut stream) = match resolver.resolve_request().await {
            Ok(request) => request,
            Err(e) => {
                error!("request failed: {}", e.to_string());
                continue;
            }
        };

        let ext = req.extensions();
        let method = req.method();

        if method == &Method::CONNECT && ext.get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT) {
            let session =
                h3_webtransport::server::WebTransportSession::accept(req, stream, conn).await?;

            handle_session(session).await?;

            return Ok(());
        }

        match stream
            .send_response(http::Response::builder().status(200).body(()).unwrap())
            .await
        {
            Ok(_) => info!("sent 200 ok"),
            Err(e) => error!("unable to send response: {:?}", e),
        };
    }

    Ok(())
}

async fn handle_session(
    _session: h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>,
) -> Result<(), anyhow::Error> {
    Ok(())
}
