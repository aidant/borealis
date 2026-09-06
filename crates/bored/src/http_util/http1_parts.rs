use http::{StatusCode, request};
use tracing::debug;

use crate::http_util::{
    HOP_BY_HOP_HEADER_NAMES, PROXY_HEADER_PREFIXES, remove_headers_by_name,
    remove_headers_by_name_and_prefix,
};

pub fn request_parts_to_http1_request_parts(
    mut parts: request::Parts,
    body_size_hint: Option<u64>,
) -> Result<request::Parts, StatusCode> {
    let old_version = parts.version;
    let new_version = parts.version = http::Version::HTTP_11;

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
            debug!(
                "Unable to convert {:?} into {:?} unable to get authority",
                old_version, new_version
            );

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
                "Unable to convert {:?} into {:?} error building path: {:?}",
                old_version, new_version, e
            );
            StatusCode::BAD_REQUEST
        })?;

    parts.headers.insert(
        "host",
        authority.parse().map_err(|e| {
            debug!(
                "Unable to convert {:?} into {:?} error parsing authority into header: {:?}",
                old_version, new_version, e
            );
            StatusCode::BAD_REQUEST
        })?,
    );

    let content_length = parts
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|content_length| content_length.to_str().ok())
        .and_then(|content_length| content_length.parse::<u64>().ok());

    remove_headers_by_name_and_prefix(
        &mut parts.headers,
        HOP_BY_HOP_HEADER_NAMES,
        PROXY_HEADER_PREFIXES,
    );

    match body_size_hint.or(content_length) {
        Some(content_length) => {
            parts.headers.insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from(content_length),
            );
        }
        None => {
            parts.headers.remove(http::header::CONTENT_LENGTH);
            parts.headers.insert(
                http::header::TRANSFER_ENCODING,
                http::HeaderValue::from_static("chunked"),
            );
        }
    };

    Ok(parts)
}

pub fn http1_response_parts_to_response_parts(
    mut parts: http::response::Parts,
) -> Result<http::response::Parts, StatusCode> {
    remove_headers_by_name(&mut parts.headers, HOP_BY_HOP_HEADER_NAMES);

    Ok(parts)
}
