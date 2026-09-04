use axum::{Extension, Router, response::IntoResponse};
use http::StatusCode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Server {
    Https,
    Http3,
}

async fn fallback(server: Option<Extension<Server>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        match server {
            Some(Extension(Server::Https)) => "served via HTTP/1.1 or HTTP/2",
            Some(Extension(Server::Http3)) => "served via HTTP/3",
            None => "served via HTTP/1.1 or HTTP/2 or HTTP/3",
        }
        .to_string(),
    )
}

pub async fn init() -> Result<Router, anyhow::Error> {
    let app = Router::new().fallback(fallback);

    Ok(app)
}
