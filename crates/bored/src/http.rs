use std::{net::SocketAddr, sync::Arc};

use axum::{
    Router,
    extract::{Path, State},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use axum_extra::{TypedHeader, headers::Host};
use http::{
    StatusCode, Uri,
    uri::{InvalidUri, InvalidUriParts},
};
use tokio::net::TcpListener;
use tracing::{error, info};
use url::Url;

use crate::acme_util::ChallengeService;

pub async fn start_server(
    addr: SocketAddr,
    https_addr: SocketAddr,
    challenge_service: Arc<ChallengeService>,
) -> Result<(), anyhow::Error> {
    let app = Router::new()
        .route("/.well-known/acme-challenge/{token}", get(challenge))
        .fallback(move |host: TypedHeader<Host>, uri: Uri| async move {
            redirect_to_https(host, uri, https_addr.port())
        })
        .with_state(challenge_service);

    let listener = TcpListener::bind(addr).await?;

    info!("HTTP server listening on: {}", listener.local_addr()?);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn challenge(
    State(challenge_service): State<Arc<ChallengeService>>,
    Path(token): Path<String>,
) -> Result<String, StatusCode> {
    challenge_service
        .complete_challenge(&token)
        .ok_or(StatusCode::NOT_FOUND)
}

fn redirect_to_https(TypedHeader(host): TypedHeader<Host>, uri: Uri, https_port: u16) -> Response {
    match http_to_https(host.hostname(), uri, https_port) {
        Ok(url) => {
            let mut response = Redirect::permanent(&url).into_response();

            response.headers_mut().insert(
                "alt-svc",
                format!(r#"h3=":{}"; ma=3600"#, https_port).parse().unwrap(),
            );

            response
        }
        Err(error) => {
            error!(?error, "HTTP to HTTPS redirect failed");

            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum HttpToHttpsError {
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error(transparent)]
    InvalidUriParts(#[from] InvalidUriParts),
    #[error(transparent)]
    ParseError(#[from] url::ParseError),
    #[error("unable to set scheme")]
    Scheme,
    #[error("unable to set port")]
    Port,
}

fn http_to_https(host: &str, uri: Uri, https_port: u16) -> Result<String, HttpToHttpsError> {
    let mut parts = uri.into_parts();

    parts.scheme = Some(axum::http::uri::Scheme::HTTPS);

    if parts.path_and_query.is_none() {
        parts.path_and_query = Some("/".parse().unwrap());
    }

    parts.authority = Some(host.parse()?);

    let mut url = Url::parse(&Uri::from_parts(parts)?.to_string())?;

    url.set_scheme("https")
        .map_err(|_| HttpToHttpsError::Scheme)?;
    url.set_port(Some(https_port))
        .map_err(|_| HttpToHttpsError::Port)?;

    Ok(url.to_string())
}
