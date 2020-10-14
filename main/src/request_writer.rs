use crate::model::*;
use async_std::prelude::*;
use encoding::EncodingRef;
use http::header::{HeaderMap, HeaderName, ACCEPT_ENCODING, CONTENT_LENGTH, HOST};
use smallvec::{smallvec, SmallVec};
use std::collections::HashSet;

pub async fn write_request(
    rw: &mut Connection,
    req: &Request,
    config: &ClientConfig,
) -> InternalRequestResult<()> {
    //

    let mut default_headers = HeaderMap::new();
    default_headers.insert(HOST, config.host.parse().unwrap());
    default_headers.insert(ACCEPT_ENCODING, "gzip".parse().unwrap());

    if let Some(body) = &req.body {
        default_headers.insert(CONTENT_LENGTH, body.len().to_string().parse().unwrap());
    }

    // default_headers.insert(ACCEPT_ENCODING, "br".parse().unwrap()); // no encoding for now

    let mut http_reg: Vec<u8> = Vec::with_capacity(2048);

    let write_url_encoded = |text: &str,
                             do_not_eacape: SmallVec<[char; 5]>|
     -> std::result::Result<Vec<u8>, InternalRequestError> {
        url_encode(text, config.url_encoding, do_not_eacape).map_err(|_| {
            InternalRequestError::UnrecoverableError(Error {
                text: format!("unable to do url encoding for text '{}'", text),
            })
        })
    };

    http_reg.extend(req.method.to_str().as_bytes());
    http_reg.extend(b" ");
    http_reg.extend(config.url_prefix.as_bytes());
    http_reg.extend(if req.path.starts_with("/") {
        if config.do_url_encoding {
            write_url_encoded(&req.path[1..], smallvec!['/'])?
        } else {
            req.path[1..].as_bytes().to_vec()
        }
    } else {
        if config.do_url_encoding {
            write_url_encoded(&req.path[0..], smallvec!['/'])?
        } else {
            req.path[0..].as_bytes().to_vec()
        }
    });

    if req.request_args.len() > 0 {
        http_reg.extend(b"?");
        for (n, (name, value)) in req.request_args.iter().enumerate() {
            http_reg.extend(if config.do_url_encoding {
                write_url_encoded(name, smallvec![])?
            } else {
                name.as_bytes().to_vec()
            });
            http_reg.extend(b"=");
            http_reg.extend(if config.do_url_encoding {
                write_url_encoded(value, smallvec![])?
            } else {
                value.as_bytes().to_vec()
            });
            if n < req.request_args.len() - 1 {
                http_reg.extend(b"&");
            }
        }
    }
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

    log::trace!("http head:\n{}", String::from_utf8_lossy(&http_reg));

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

fn url_encode(
    data: &str,
    encoding: EncodingRef,
    do_not_esacape: SmallVec<[char; 5]>,
) -> std::result::Result<Vec<u8>, ()> {
    use encoding::EncoderTrap;
    let mut result: Vec<u8> = Vec::new();
    for b in data.chars() {
        match b {
            // Accepted characters
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.extend(b.to_string().as_bytes())
            }

            // Everything else is percent-encoded
            b => {
                if do_not_esacape.contains(&b) {
                    result.extend(b.to_string().as_bytes())
                } else {
                    let bytes = encoding
                        .encode(&b.to_string(), EncoderTrap::Strict)
                        .map_err(|_| ())?;
                    for b in bytes {
                        result.extend(format!("%{:02X}", b as u32).as_bytes())
                    }
                }
            }
        };
    }

    Ok(result)
}

#[test]
fn it_works() {
    fn test_encode(data: &str, encoding: EncodingRef) -> String {
        String::from_utf8_lossy(&url_encode(data, encoding, SmallVec::new()).unwrap()).into()
    }

    assert_eq!(
        &test_encode("m üöä", encoding::all::ISO_8859_1),
        "m%20%FC%F6%E4"
    );
    assert_eq!(
        &test_encode("m üöä", encoding::all::UTF_8),
        "m%20%C3%BC%C3%B6%C3%A4"
    );
    assert_eq!(
        &test_encode("Привет мир", encoding::all::UTF_8),
        "%D0%9F%D1%80%D0%B8%D0%B2%D0%B5%D1%82%20%D0%BC%D0%B8%D1%80"
    );
}
