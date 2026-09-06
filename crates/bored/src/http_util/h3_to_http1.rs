use bytes::Bytes;
use h3::{error::StreamError, server::RequestStream};
use http::{Request, StatusCode};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::error;

use crate::http_util::{
    h3_recv_body::h3_recv_body, h3_send_body::h3_send_body, http1_response_parts_to_response_parts,
    request_parts_to_http1_request_parts,
};

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
    let (mut h3_send, h3_recv) = h3_stream.split();
    let (h3_request_parts, _) = h3_request.into_parts();

    let http1_request_parts = match request_parts_to_http1_request_parts(h3_request_parts, None) {
        Ok(parts) => parts,
        Err(status) => return Ok(send_error(h3_send, status).await?),
    };

    let request = hyper::Request::from_parts(http1_request_parts, h3_recv_body(h3_recv));

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

    let h3_response_parts = match http1_response_parts_to_response_parts(http1_response_parts) {
        Ok(parts) => parts,
        Err(status) => {
            return Ok(send_error(h3_send, status).await?);
        }
    };

    h3_send
        .send_response(http::Response::from_parts(h3_response_parts, ()))
        .await?;

    h3_send_body::<S, _>(&mut h3_send, http1_response_body).await?;

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
