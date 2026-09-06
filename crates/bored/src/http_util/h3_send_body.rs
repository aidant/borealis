use bytes::Bytes;
use h3::server::RequestStream;
use h3_util::server_body::send_h3_server_body;
use hyper::body::Body;

pub async fn h3_send_body<S, B>(
    h3_send: &mut RequestStream<<S as h3::quic::BidiStream<Bytes>>::SendStream, Bytes>,
    body: B,
) -> Result<(), anyhow::Error>
where
    B: Body + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
    <B as Body>::Data: Send + Sync,
    S: h3::quic::BidiStream<Bytes>,
{
    send_h3_server_body::<B, S>(h3_send, body)
        .await
        .map_err(anyhow::Error::from_boxed)?;

    Ok(())
}
