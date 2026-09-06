use bytes::Bytes;
use http::StatusCode;
use http_body_util::BodyExt;
use hyper::body::Body;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::error;

use crate::http_util::{
    http1_response_parts_to_response_parts, request_parts_to_http1_request_parts,
};

pub type ProxiedResponse =
    hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>>;

pub async fn hyper_to_http1<B, I>(hyper_request: hyper::Request<B>, http1_io: I) -> ProxiedResponse
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: std::error::Error + Send + Sync + 'static,
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (hyper_request_parts, body) = hyper_request.into_parts();

    let http1_request_parts =
        match request_parts_to_http1_request_parts(hyper_request_parts, body.size_hint().exact()) {
            Ok(parts) => parts,
            Err(status) => return build_error(status),
        };

    let request = hyper::Request::from_parts(http1_request_parts, body);

    let (mut http1_sender, http1_connection) =
        match hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(http1_io)).await {
            Ok(handshake) => handshake,
            Err(e) => {
                error!("HTTP/1.1 WebTransport handshake failed: {:?}", e);
                return build_error(StatusCode::BAD_GATEWAY);
            }
        };

    tokio::spawn(async move {
        if let Err(e) = http1_connection.await {
            error!("HTTP/1.1 WebTransport connection failed: {:?}", e);
        }
    });

    let response = match http1_sender.send_request(request).await {
        Ok(response) => response,
        Err(e) => {
            error!("HTTP/1.1 WebTransport request failed: {:?}", e);
            return build_error(StatusCode::BAD_GATEWAY);
        }
    };

    let (http1_response_parts, http1_response_body) = response.into_parts();

    let h1_response_parts = match http1_response_parts_to_response_parts(http1_response_parts) {
        Ok(parts) => parts,
        Err(status) => return build_error(status),
    };

    hyper::Response::from_parts(h1_response_parts, http1_response_body.boxed())
}

fn build_error(status: StatusCode) -> ProxiedResponse {
    hyper::Response::builder()
        .status(status)
        .body(
            http_body_util::Full::<Bytes>::new(Bytes::new())
                .map_err(|error| match error {})
                .boxed(),
        )
        .unwrap()
}
