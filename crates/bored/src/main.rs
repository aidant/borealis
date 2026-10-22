use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use tracing_subscriber::fmt::format::FmtSpan;

use crate::{
    acme_util::{AccountService, CertificateService, ChallengeService, OrderService},
    tls_util::DynamicCertResolver,
    web_transport::WebTransportRegistry,
};

mod acme_util;
mod app;
mod http;
mod http3;
mod http_util;
mod https;
mod routes;
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
    let http_addr: SocketAddr = "127.0.0.1:8080".parse()?;

    let dynamic_cert_resolver = Arc::new(DynamicCertResolver::new());
    let router = app::init(dynamic_cert_resolver.to_owned()).await?;
    let registry = WebTransportRegistry::new();

    let account_credentials_path = PathBuf::from("./data/acme/account.json");
    let certificate_cache_path = PathBuf::from("./data/acme/certificates/");

    let account_service = Arc::new(AccountService::new(account_credentials_path));
    let challenge_service = Arc::new(ChallengeService::new());
    let order_service = Arc::new(OrderService::new(
        account_service.clone(),
        challenge_service.clone(),
    ));
    let certificate_service = CertificateService::new(
        certificate_cache_path,
        dynamic_cert_resolver.to_owned(),
        order_service,
    );

    account_service.init_from_disk().await?;
    certificate_service.init_from_disk().await?;

    tokio::try_join!(
        http::start_server(http_addr, server_addr, challenge_service),
        https::start_server(
            server_addr,
            dynamic_cert_resolver.to_owned(),
            router.clone(),
            registry.to_owned()
        ),
        async {
            Ok(http3::start_server(server_addr, dynamic_cert_resolver, router, registry).await?)
        }
    )?;

    Ok(())
}
