use bytes::Bytes;
use h3::server::RequestStream;
use h3_util::server_body::send_h3_server_body;

pub async fn h3_send_body<S>(
    h3_send: &mut RequestStream<<S as h3::quic::BidiStream<Bytes>>::SendStream, Bytes>,
    body: hyper::body::Incoming,
) -> Result<(), anyhow::Error>
where
    S: h3::quic::BidiStream<Bytes>,
{
    send_h3_server_body::<hyper::body::Incoming, S>(h3_send, body)
        .await
        .map_err(anyhow::Error::from_boxed)?;

    Ok(())
}
