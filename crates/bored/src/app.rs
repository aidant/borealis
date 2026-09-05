use std::ops::Deref;

use axum::{
    Extension, Router,
    response::{Html, IntoResponse},
    routing::get,
};
use http::StatusCode;
use rustls::pki_types::CertificateDer;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy)]
pub enum ServerImpl {
    Https,
    Http3,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ServerName(pub String);

impl Deref for ServerName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

async fn fallback(
    server_impl: Option<Extension<ServerImpl>>,
    server_name: Option<Extension<ServerName>>,
) -> impl IntoResponse {
    (
        StatusCode::OK,
        format!(
            "{} served via {}",
            match server_name {
                Some(Extension(server_name)) => server_name.to_string(),
                None => "<unknown>".to_string(),
            },
            match server_impl {
                Some(Extension(ServerImpl::Https)) => "HTTP/1.1 or HTTP/2",
                Some(Extension(ServerImpl::Http3)) => "HTTP/3",
                None => "HTTP/1.1 or HTTP/2 or HTTP/3",
            }
        ),
    )
}

pub async fn init(cert: CertificateDer<'static>) -> Result<Router, anyhow::Error> {
    let mut hasher = Sha256::new();
    hasher.update(cert);
    let result = hasher.finalize();

    let index_html = include_str!("./index.html").replace(
        "new Uint8Array([])",
        &format!("new Uint8Array({:?})", result.as_slice()),
    );

    let app = Router::new()
        .route("/", get(async || Html(index_html)))
        .fallback(fallback);

    Ok(app)
}
