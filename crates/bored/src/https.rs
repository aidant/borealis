use std::{net::SocketAddr, sync::Arc};

use axum::{
    Router,
    extract::Request,
    middleware::{self, Next},
};
use bytes::Bytes;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    service::TowerToHyperService,
};
use rustls::server::ResolvesServerCert;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tracing::error;

use crate::{
    app::{ServerImpl, ServerName},
    web_transport::WebTransportRegistry,
};

pub async fn start_server(
    addr: SocketAddr,
    cert_resolver: Arc<dyn ResolvesServerCert>,
    app: Router,
    registry: WebTransportRegistry,
) -> Result<(), anyhow::Error> {
    let app = app.layer(middleware::from_fn(
        move |mut req: Request, next: Next| async move {
            req.extensions_mut().insert(ServerImpl::Https);

            let mut response = next.run(req).await;

            response.headers_mut().insert(
                "alt-svc",
                format!(r#"h3=":{}"; ma=3600"#, addr.port())
                    .parse()
                    .unwrap(),
            );

            response
        },
    ));

    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(cert_resolver);
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = TcpListener::bind(addr).await?;

    loop {
        if let Err(e) = accept_tls_stream(
            acceptor.clone(),
            &listener,
            app.to_owned(),
            registry.to_owned(),
        )
        .await
        {
            error!("accepting HTTPS connection failed: {:?}", e);
            continue;
        };
    }
}

async fn accept_tls_stream(
    acceptor: TlsAcceptor,
    listener: &TcpListener,
    app: Router,
    registry: WebTransportRegistry,
) -> Result<(), anyhow::Error> {
    let (stream, _addr) = listener.accept().await?;
    let stream = acceptor.accept(stream).await?;

    let (_io, session) = stream.get_ref();

    let server_name = session
        .server_name()
        .map(|server_name| ServerName(server_name.to_owned()));

    if let Some(server_name) = server_name.clone() {
        if let Some(tx) = registry.requests.get(&server_name) {
            let future = bind_tcp_stream_to_web_transport(tx.to_owned(), stream);

            tokio::spawn(async move {
                if let Err(e) = future.await {
                    error!("HTTPS request failed: {:?}", e);
                }
            });

            return Ok(());
        };
    };

    let app = app.layer(middleware::from_fn(move |mut req: Request, next: Next| {
        let server_name = server_name.clone();

        async move {
            if let Some(server_name) = server_name {
                req.extensions_mut().insert(server_name);
            };

            next.run(req).await
        }
    }));

    let future = accept_tcp_stream(stream, app);

    tokio::spawn(async move {
        if let Err(e) = future.await {
            error!("HTTPS connection failed: {:?}", e);
        }
    });

    Ok(())
}

async fn accept_tcp_stream(stream: TlsStream<TcpStream>, app: Router) -> Result<(), anyhow::Error> {
    if let Err(err) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection_with_upgrades(TokioIo::new(stream), TowerToHyperService::new(app))
        .await
    {
        error!("failed to serve connection: {err:#}");
    }

    Ok(())
}

async fn bind_tcp_stream_to_web_transport(
    tx: mpsc::Sender<
        oneshot::Sender<
            Result<
                h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>,
                anyhow::Error,
            >,
        >,
    >,
    stream: TlsStream<TcpStream>,
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

    let mut web_transport_stream = resp_rx.await??;

    Ok(())
}
