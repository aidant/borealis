use bytes::Bytes;
use h3::server::RequestStream;

use crate::http_util::h3_recv_body::{H3Body, h3_recv_body};
use crate::http_util::h3_send_body::h3_send_body;

pub async fn h3_server<S, B>(
    h3_request: http::Request<()>,
    h3_stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    hyper_service: S,
) -> Result<(), anyhow::Error>
where
    S: hyper::service::Service<
            hyper::Request<H3Body<h3_quinn::RecvStream>>,
            Response = hyper::Response<B>,
        >,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: hyper::body::Body + Send + 'static,
    B::Data: Send + Sync,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let (mut h3_send, h3_recv) = h3_stream.split();

    let (h3_request_parts, _) = h3_request.into_parts();

    let request = hyper::Request::from_parts(h3_request_parts, h3_recv_body(h3_recv));
    let response = hyper::service::Service::call(&hyper_service, request)
        .await
        .map_err(|error| anyhow::Error::from_boxed(error.into()))?;

    let (response_parts, response_body) = response.into_parts();

    h3_send
        .send_response(hyper::Response::from_parts(response_parts, ()))
        .await?;

    h3_send_body::<h3_quinn::BidiStream<Bytes>, _>(&mut h3_send, response_body).await?;

    Ok(())
}
