use std::{ops::Deref, sync::Arc};

use axum::{
    Extension, Json, Router,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use hostaddr::Domain;
use http::StatusCode;
use sha2::{Digest, Sha256};

use crate::{routes, tls_util::DynamicCertResolver};

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

async fn create_portal() -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

pub async fn init(
    dynamic_cert_resolver: Arc<DynamicCertResolver>,
) -> Result<Router, anyhow::Error> {
    let app = Router::new();
    let app = routes::create_portal::init(app);
    let app = routes::list_portals::init(app);
    let app = routes::get_portal::init(app);
    let app = routes::delete_portal::init(app);
    let app = routes::portal_connect::init(app);
    let app = app.route(
            "/",
            get(async move |server_name: Option<Extension<ServerName>>| -> Result<Response, (StatusCode, &'static str)> {
                let domain_name = Domain::<String>::try_from(
                    server_name
                        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "missing server_name"))?
                        .to_string(),
                )
                .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "invalid domain_name"))?;
                let certified_key = dynamic_cert_resolver
                    .get(&domain_name)
                    .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "no certified_key"))?;
                let cert = certified_key
                    .cert
                    .first()
                    .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "no cert"))?;

                let mut hasher = Sha256::new();
                hasher.update(cert);
                let result = hasher.finalize();

                let index_html = include_str!("./index.html").replace(
                    "new Uint8Array([])",
                    &format!("new Uint8Array({:?})", result.as_slice()),
                );

                Ok(Html(index_html).into_response())
            }),
        )
        .fallback(fallback);

    Ok(app)
}
