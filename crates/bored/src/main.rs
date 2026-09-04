use std::{net::SocketAddr, sync::Arc};

use rustls::{
    pki_types::PrivateKeyDer,
    sign::{CertifiedKey, SingleCertAndKey},
};
use sha2::{Digest, Sha256};
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::web_transport::WebTransportRegistry;

mod app;
mod http3;
mod https;
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
    info!("value: new Uint8Array({})", js_array_bytes);

    let certified_key = CertifiedKey::from_der(
        vec![cert.der().to_owned()],
        PrivateKeyDer::try_from(signing_key.serialize_der()).unwrap(),
        &rustls::crypto::aws_lc_rs::default_provider(),
    )
    .unwrap();

    let router = app::init().await?;
    let registry = WebTransportRegistry::new();

    tokio::try_join!(
        https::start_server(
            server_addr,
            Arc::new(SingleCertAndKey::from(certified_key.clone())),
            router.clone(),
            registry.to_owned()
        ),
        async {
            Ok(http3::start_server(
                server_addr,
                Arc::new(SingleCertAndKey::from(certified_key)),
                router,
                registry,
            )
            .await?)
        }
    )?;

    Ok(())
}
