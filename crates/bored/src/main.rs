use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use h3::ext::Protocol;
use http::Method;
use hyper_util::rt::{TokioExecutor, TokioIo};
use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
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

    let server_addr: SocketAddr = "127.0.0.1:8443".parse()?;

    let now = time::OffsetDateTime::now_utc();

    let mut cert_params =
        rcgen::CertificateParams::new(vec!["127.0.0.1".into(), "localhost".into()])?;
    cert_params.not_before = now;
    cert_params.not_after = now + time::Duration::days(7);
    let signing_key = rcgen::KeyPair::generate()?;
    let cert = cert_params.self_signed(&signing_key)?;

    let mut hasher = Sha256::new();
    hasher.update(&cert.der());
    let result = hasher.finalize();

    let js_array_bytes = format!("{:?}", result.as_slice());
    info!("Copy this into your JS client `value` field:");
    info!("value: new Uint8Array({})", js_array_bytes);

    let cert_chain = vec![cert.der().clone()];
    let key_der: PrivateKeyDer<'static> = signing_key.serialize_der().try_into().unwrap();

    tokio::try_join!(
        start_h2_server(server_addr, cert_chain.clone(), key_der.clone_key()),
        start_h3_server(server_addr, cert_chain.clone(), key_der.clone_key())
    )?;

    Ok(())
}

async fn start_h2_server(
    server_addr: SocketAddr,
    cert_chain: Vec<CertificateDer<'static>>,
    key_der: PrivateKeyDer<'static>,
) -> Result<(), anyhow::Error> {
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key_der)?;

    server_crypto.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let acceptor = TlsAcceptor::from(Arc::new(server_crypto));
    let listener = TcpListener::bind(server_addr).await?;

    loop {
        let acceptor = acceptor.clone();
        let (stream, _peer_addr) = match listener.accept().await {
            Ok((stream, peer_addr)) => (stream, peer_addr),
            Err(e) => {
                error!("accepting connection failed: {:?}", e);
                continue;
            }
        };

        let stream = match acceptor.accept(stream).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("accepting connection failed: {:?}", e);
                continue;
            }
        };

        tokio::spawn(async move {
            if let Err(e) = handle_h2_connection(stream).await {
                error!("connection failed: {:?}", e);
            }
        });
    }
}

async fn start_h3_server(
    server_addr: SocketAddr,
    cert_chain: Vec<CertificateDer<'static>>,
    key_der: PrivateKeyDer<'static>,
) -> Result<(), anyhow::Error> {
    let mut server_crypto =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)?;

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
            if let Err(e) = handle_h3_connection(conn).await {
                error!("connection failed: {:?}", e);
            }
        });
    }

    Ok(())
}

async fn handle_h2_connection(stream: TlsStream<TcpStream>) -> Result<(), anyhow::Error> {
    let service = hyper::service::service_fn(move |_req| async move {
        let response = http::Response::builder()
            .status(200)
            .header("alt-svc", format!("h3=\":8443\"; ma=3600"))
            .body(http_body_util::Full::new(Bytes::from("")))
            .unwrap();

        Ok::<_, anyhow::Error>(response)
    });

    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(stream), service)
        .await?;

    Ok(())
}

async fn handle_h3_connection(conn: quinn::Incoming) -> Result<(), anyhow::Error> {
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
                error!("connection failed: {:?}", e);
                break;
            }
        };

        let (req, mut stream) = match resolver.resolve_request().await {
            Ok(request) => request,
            Err(e) => {
                error!("request failed: {:?}", e);
                continue;
            }
        };

        let ext = req.extensions();
        let method = req.method();

        if method == &Method::CONNECT && ext.get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT) {
            let session =
                h3_webtransport::server::WebTransportSession::accept(req, stream, conn).await?;

            handle_web_transport_session(session).await?;

            return Ok(());
        }

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

    Ok(())
}

async fn handle_web_transport_session(
    session: h3_webtransport::server::WebTransportSession<h3_quinn::Connection, Bytes>,
) -> Result<(), anyhow::Error> {
    while let Some(accepted) = session.accept_bi().await? {
        tokio::spawn(async move {
            match accepted {
                h3_webtransport::server::AcceptedBi::BidiStream(session_id, mut stream) => {
                    let mut buf = vec![0; 1024];
                    while let Ok(n) = stream.read(&mut buf).await {
                        stream.write_all(&buf[..n]).await.ok();
                    }
                }
                h3_webtransport::server::AcceptedBi::Request(request, request_stream) => todo!(),
            }
        });
    }

    Ok(())
}
