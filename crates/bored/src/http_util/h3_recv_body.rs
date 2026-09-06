use bytes::Bytes;
use h3::server::RequestStream;
use h3_util::server_body::H3IncomingServer;
use hyper::body::Body;

pub struct H3Body<S>
where
    S: h3::quic::RecvStream,
{
    inner: H3IncomingServer<S, Bytes>,
}

pub fn h3_recv_body<S>(h3_recv: RequestStream<S, Bytes>) -> H3Body<S>
where
    S: h3::quic::RecvStream,
{
    H3Body {
        inner: H3IncomingServer::new(h3_recv),
    }
}

impl<S> Body for H3Body<S>
where
    S: h3::quic::RecvStream,
{
    type Data = Bytes;

    type Error = h3::error::StreamError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        inner.poll_frame(cx)
    }
}
