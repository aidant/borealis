use std::{net::SocketAddr, sync::Arc};

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

use crate::app::{ServerImpl, ServerName};

pub async fn start_server(
    addr: SocketAddr,
    cert_resolver: Arc<dyn ResolvesServerCert>,
    app: Router,
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
        if let Err(e) = accept_tls_stream(acceptor.clone(), &listener, app.to_owned()).await {
            error!("accepting HTTPS connection failed: {:?}", e);
            continue;
        };
    }
}

async fn accept_tls_stream(
    acceptor: TlsAcceptor,
    listener: &TcpListener,
    app: Router,
) -> Result<(), anyhow::Error> {
    let (stream, _addr) = listener.accept().await?;
    let stream = acceptor.accept(stream).await?;

    let (_io, session) = stream.get_ref();

    let server_name = session.server_name().map(|x| x.to_owned());

    let app = app.layer(middleware::from_fn(move |mut req: Request, next: Next| {
        let server_name = server_name.clone();

        async move {
            if let Some(server_name) = server_name {
                req.extensions_mut().insert(ServerName(server_name));
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
