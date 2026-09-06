use http::{HeaderMap, HeaderName};

pub const HOP_BY_HOP_HEADER_NAMES: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub const PROXY_HEADER_PREFIXES: &[&str] = &["x-forwarded-"];

pub fn remove_headers<F>(headers: &mut HeaderMap, predicate: F)
where
    F: FnMut(&HeaderName) -> bool,
{
    let headers_to_remove = headers
        .keys()
        .cloned()
        .filter(predicate)
        .collect::<Vec<HeaderName>>();

    for header in headers_to_remove {
        headers.remove(header);
    }
}

pub fn remove_headers_by_name(headers: &mut HeaderMap, names: &[&str]) {
    remove_headers(headers, |name| {
        let name = name.as_str().to_lowercase();

        names.iter().any(|header| &&name == header)
    })
}

pub fn remove_headers_by_name_and_prefix(
    headers: &mut HeaderMap,
    names: &[&str],
    prefixes: &[&str],
) {
    remove_headers(headers, |name| {
        let name = name.as_str().to_lowercase();

        names.iter().any(|header| &&name == header)
            || prefixes.iter().any(|header| name.starts_with(header))
    })
}
