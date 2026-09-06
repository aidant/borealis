use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::Request,
    middleware::{self, Next},
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    service::TowerToHyperService,
};
use rustls::server::ResolvesServerCert;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tracing::error;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

use crate::{
    app::{ServerImpl, ServerName},
    http_util::{ProxiedResponse, hyper_to_http1},
    web_transport::{WebTransportRegistry, WebTransportStreamReceiver},
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
        let (stream, _addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                error!("accepting TCP connection failed: {:?}", e);
                continue;
            }
        };

        let future = accept_tls_stream(
            acceptor.clone(),
            stream,
            app.to_owned(),
            registry.to_owned(),
        );

        tokio::spawn(async move {
            if let Err(e) = future.await {
                error!("accepting HTTPS connection failed: {:?}", e);
            }
        });
    }
}

async fn accept_tls_stream(
    acceptor: TlsAcceptor,
    stream: TcpStream,
    app: Router,
    registry: WebTransportRegistry,
) -> Result<(), anyhow::Error> {
    let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream))
        .await
        .map_err(|_| anyhow::anyhow!("TLS handshake timed out after {HANDSHAKE_TIMEOUT:?}"))??;

    let (_io, session) = stream.get_ref();

    let server_name = session
        .server_name()
        .map(|server_name| ServerName(server_name.to_owned()));

    if let Some(server_name) = server_name.clone() {
        if let Some(web_transport_stream_receiver) = registry.get(&server_name) {
            let future = bind_tcp_stream_to_web_transport(web_transport_stream_receiver, stream);

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
    web_transport_stream_receiver: WebTransportStreamReceiver,
    stream: TlsStream<TcpStream>,
) -> Result<(), anyhow::Error> {
    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection_with_upgrades(
            TokioIo::new(stream),
            TowerToHyperService::new(tower::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let web_transport_stream_receiver = web_transport_stream_receiver.clone();

                    async move {
                        bind_request_to_web_transport(web_transport_stream_receiver, request).await
                    }
                },
            )),
        )
        .await
        .map_err(anyhow::Error::from_boxed)?;

    Ok(())
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
