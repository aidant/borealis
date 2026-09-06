use std::{net::SocketAddr, sync::Arc};

use tracing_subscriber::fmt::format::FmtSpan;

use crate::{tls_util::DynamicCertResolver, web_transport::WebTransportRegistry};

mod acme_util;
mod app;
mod http3;
mod http_util;
mod https;
mod tls_util;
mod web_transport;

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

    let dynamic_cert_resolver = DynamicCertResolver::new();

    let resolver = Arc::new(dynamic_cert_resolver);
    let router = app::init(resolver.to_owned()).await?;
    let registry = WebTransportRegistry::new();

    tokio::try_join!(
        https::start_server(
            server_addr,
            resolver.to_owned(),
            router.clone(),
            registry.to_owned()
        ),
        async { Ok(http3::start_server(server_addr, resolver, router, registry).await?) }
    )?;

    Ok(())
}
