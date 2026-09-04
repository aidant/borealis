use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::server::ResolvesServerCert;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tracing::error;

pub async fn start_server(
    addr: SocketAddr,
    cert_resolver: Arc<dyn ResolvesServerCert>,
) -> Result<(), anyhow::Error> {
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(cert_resolver);
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = TcpListener::bind(addr).await?;

    loop {
        if let Err(e) = accept_tls_stream(addr, acceptor.clone(), &listener).await {
            error!("accepting HTTPS connection failed: {:?}", e);
            continue;
        };
    }
}

async fn accept_tls_stream(
    addr: SocketAddr,
    acceptor: TlsAcceptor,
    listener: &TcpListener,
) -> Result<(), anyhow::Error> {
    let (stream, _addr) = listener.accept().await?;
    let stream = acceptor.accept(stream).await?;

    let future = accept_tcp_stream(addr, stream);

    tokio::spawn(async move {
        if let Err(e) = future.await {
            error!("HTTPS connection failed: {:?}", e);
        }
    });

    Ok(())
}

async fn accept_tcp_stream(
    addr: SocketAddr,
    stream: TlsStream<TcpStream>,
) -> Result<(), anyhow::Error> {
    let service = hyper::service::service_fn(move |_req| async move {
        let response = http::Response::builder()
            .status(200)
            .header("alt-svc", format!("h3=\":{}\"; ma=3600", addr.port()))
            .body(http_body_util::Full::new(Bytes::from("")))
            .unwrap();

        Ok::<_, anyhow::Error>(response)
    });

    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(stream), service)
        .await?;

    Ok(())
}
