use bytes::Bytes;
use h3::{error::StreamError, server::RequestStream};
use h3_util::server_body::H3IncomingServer;
use http::{HeaderName, Request, StatusCode};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, error};

const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

const RESTRICTED_REQUEST_HEADERS: &[&str] = &["content-length"];
const RESTRICTED_REQUEST_HEADER_PREFIX: &[&str] = &["x-forwarded-"];

fn h3_request_parts_to_http1_request_parts(
    mut parts: http::request::Parts,
) -> Result<http::request::Parts, StatusCode> {
    parts.version = http::Version::HTTP_11;

    let authority = parts
        .uri
        .authority()
        .map(|authority| authority.as_str().to_owned())
        .or_else(|| {
            parts
                .headers
                .get("host")
                .and_then(|host| host.to_str().ok().map(str::to_owned))
        })
        .ok_or_else(|| {
            debug!("Unable to convert HTTP/3 into HTTP/1.1 unable to get authority");

            StatusCode::BAD_REQUEST
        })?;

    parts.uri = http::Uri::builder()
        .path_and_query(
            parts
                .uri
                .path_and_query()
                .map(|path_and_query| path_and_query.as_str())
                .unwrap_or("/"),
        )
        .build()
        .map_err(|e| {
            debug!(
                "Unable to convert HTTP/3 into HTTP/1.1 error building path: {:?}",
                e
            );
            StatusCode::BAD_REQUEST
        })?;

    parts.headers.insert(
        "host",
        authority.parse().map_err(|e| {
            debug!(
                "Unable to convert HTTP/3 into HTTP/1.1 error parsing authority into header: {:?}",
                e
            );
            StatusCode::BAD_REQUEST
        })?,
    );

    let content_length = parts
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|content_length| content_length.to_str().ok())
        .and_then(|content_length| content_length.parse::<u64>().ok());

    let headers_to_remove = parts
        .headers
        .keys()
        .filter(|name| {
            let name = name.as_str().to_lowercase();

            HOP_BY_HOP_HEADERS
                .iter()
                .chain(RESTRICTED_REQUEST_HEADERS)
                .any(|header| &&name == header)
                || RESTRICTED_REQUEST_HEADER_PREFIX
                    .iter()
                    .any(|header| name.starts_with(header))
        })
        .cloned()
        .collect::<Vec<HeaderName>>();

    for name in headers_to_remove {
        parts.headers.remove(name);
    }

    match content_length {
        Some(content_length) => parts.headers.insert(
            http::header::CONTENT_LENGTH,
            http::HeaderValue::from(content_length),
        ),
        None => parts.headers.insert(
            http::header::TRANSFER_ENCODING,
            http::HeaderValue::from_static("chunked"),
        ),
    };

    Ok(parts)
}

fn http1_response_parts_to_h3_response_parts(
    mut parts: http::response::Parts,
) -> Result<http::response::Parts, StatusCode> {
    let headers_to_remove = parts
        .headers
        .keys()
        .filter(|name| {
            let name = name.as_str().to_lowercase();

            HOP_BY_HOP_HEADERS.iter().any(|header| &&name == header)
        })
        .cloned()
        .collect::<Vec<HeaderName>>();

    for name in headers_to_remove {
        parts.headers.remove(name);
    }

    Ok(parts)
}

pub async fn h3_to_http1<S, I>(
    h3_request: Request<()>,
    h3_stream: RequestStream<S, Bytes>,
    http1_io: I,
) -> Result<(), anyhow::Error>
where
    S: h3::quic::BidiStream<Bytes> + Send,
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    <S as h3::quic::BidiStream<bytes::Bytes>>::RecvStream: Send + 'static,
{
    let (h3_request_parts, _) = h3_request.into_parts();
    let http1_request_parts = match h3_request_parts_to_http1_request_parts(h3_request_parts) {
        Ok(parts) => parts,
        Err(status) => return Ok(send_error(h3_stream, status).await?),
    };

    let (mut h3_send, h3_recv) = h3_stream.split();

    let request = hyper::Request::from_parts(http1_request_parts, H3IncomingServer::new(h3_recv));

    let (mut http1_sender, http1_connection) =
        match hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(http1_io)).await {
            Ok(handshake) => handshake,
            Err(e) => {
                error!("HTTP/1.1 WebTransport handshake failed: {:?}", e);
                return Ok(send_error(h3_send, StatusCode::BAD_GATEWAY).await?);
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
            return Ok(send_error(h3_send, StatusCode::BAD_GATEWAY).await?);
        }
    };

    let (http1_response_parts, http1_response_body) = response.into_parts();

    let h3_response_parts = match http1_response_parts_to_h3_response_parts(http1_response_parts) {
        Ok(parts) => parts,
        Err(status) => {
            return Ok(send_error(h3_send, status).await?);
        }
    };

    h3_send
        .send_response(http::Response::from_parts(h3_response_parts, ()))
        .await?;

    h3_util::server_body::send_h3_server_body::<hyper::body::Incoming, S>(
        &mut h3_send,
        http1_response_body,
    )
    .await
    .map_err(anyhow::Error::from_boxed)?;

    Ok(())
}

async fn send_error<S>(
    mut h3_send: RequestStream<S, Bytes>,
    status: StatusCode,
) -> Result<(), StreamError>
where
    S: h3::quic::SendStream<Bytes>,
{
    h3_send
        .send_response(http::Response::builder().status(status).body(()).unwrap())
        .await?;
    h3_send.finish().await?;

    Ok(())
}
