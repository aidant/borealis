use axum::{Extension, Router, response::IntoResponse};
use http::StatusCode;

#[derive(Debug, Clone, Copy)]
pub enum ServerImpl {
    Https,
    Http3,
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ServerName(pub String);

async fn fallback(
    server_impl: Option<Extension<ServerImpl>>,
    server_name: Option<Extension<ServerName>>,
) -> impl IntoResponse {
    (
        StatusCode::OK,
        format!(
            "{} served via {}",
            match server_name {
                Some(Extension(server_name)) => server_name.0,
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

pub async fn init() -> Result<Router, anyhow::Error> {
    let app = Router::new().fallback(fallback);

    Ok(app)
}
