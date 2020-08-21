use crate::model::*;
use async_std::prelude::*;
use http::header::{HeaderMap, HeaderName, CONTENT_LENGTH, /*ACCEPT_ENCODING,*/ HOST};
use std::collections::HashSet;

pub async fn write_request(
    rw: &mut Connection,
    req: &Request,
    config: &ClientConfig,
) -> InternalRequestResult<()> {
    //

    let mut default_headers = HeaderMap::new();
    default_headers.insert(HOST, config.host.parse().unwrap());

    if let Some(body) = &req.body {
        default_headers.insert(CONTENT_LENGTH, body.len().to_string().parse().unwrap());
    }

    // default_headers.insert(ACCEPT_ENCODING, "br".parse().unwrap()); // no encoding for now

    let mut http_reg = Vec::with_capacity(2048);

    http_reg.extend(req.method.to_str().as_bytes());
    http_reg.extend(b" ");
    http_reg.extend(config.url_prefix.as_bytes());
    http_reg.extend(if req.path.starts_with("/") {
        req.path[1..].as_bytes()
    } else {
        req.path[0..].as_bytes()
    });
    http_reg.extend(b" HTTP/1.1\r\n");

    let req_header_names: HashSet<&HeaderName> = req.headers.iter().map(|(n, _)| n).collect();

    let send_headers = default_headers
        .iter()
        .filter(|(n, _)| !req_header_names.contains(n))
        .chain(req.headers.iter());

    for (name, value) in send_headers {
        http_reg.extend(name.as_str().as_bytes());
        http_reg.extend(b": ");
        http_reg.extend(value.as_bytes());
        http_reg.extend(b"\r\n");
    }

    http_reg.extend(b"\r\n");

    log::debug!(
        "sending {} to '{}://{}:{}{}{}'",
        req.method.to_str(),
        if config.use_tls { "https" } else { "http" },
        &config.host,
        &config.port,
        &config.url_prefix,
        if req.path.starts_with("/") {
            &req.path[1..]
        } else {
            &req.path[0..]
        },
    );

    rw.1.write_all(&http_reg)
        .await
        .map_err(|_| InternalRequestError::ConnectionIsClosed)?;

    if let Some(body) = &req.body {
        rw.1.write_all(&body).await.map_err(|_| {
            InternalRequestError::UnrecoverableError(Error {
                text: "the connection was broken while sending the request body".into(),
            })
        })?;
    }

    Ok(())
}
