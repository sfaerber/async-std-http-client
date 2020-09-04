use crate::model::*;
use async_std::prelude::*;
use http::{
    header::{HeaderName, CONNECTION /*, CONTENT_LENGTH*/, CONTENT_ENCODING},
    status::StatusCode,
    HeaderMap, HeaderValue,
};
use http_parser::*;
use libflate::gzip::Decoder;
use std::io;
use std::time::Instant;

pub async fn read_response(
    rw: &mut Connection,
    config: &ClientConfig,
    started_at: Instant,
) -> InternalRequestResult<(Response, ConnectionState)> {
    log::debug!("starting to read response");

    let mut parser = HttpParser::new(HttpParserType::Response);

    let mut cb = Callback {
        error: None,
        body: Vec::new(),
        headers: HeaderMap::with_capacity(10),
        last_header_name: None,
        status: None,
        is_message_complete: false,
        started_at,
    };

    let mut buffer: [u8; 1024 * 64] = [0; 1024 * 64];
    let mut loops: usize = 0;

    loop {
        let bytes_read = rw.1.read(&mut buffer).await.map_err(|err| {
            if loops == 0 {
                InternalRequestError::ConnectionIsClosed
            } else {
                InternalRequestError::UnrecoverableError(Error {
                    text: format!("lost connection while reading: {}", err),
                })
            }
        })?;

        if bytes_read == 0 {
            // TODO: is this fine if we are in http 1.0-mode`?
            return Err(if loops == 0 {
                InternalRequestError::ConnectionIsClosed
            } else {
                InternalRequestError::UnrecoverableError(Error {
                    text: format!("unepected EOF, loop: {}", loops),
                })
            });
        }

        parser.execute(&mut cb, &buffer[..bytes_read]);

        if let Some(err) = cb.error {
            return Err(InternalRequestError::UnrecoverableError(err));
        }

        if cb.is_message_complete {
            let response = cb
                .to_response(config.response_body_limit)
                .map_err(InternalRequestError::UnrecoverableError)?;

            let connection_state = if response
                .headers
                .get(CONNECTION)
                .iter()
                .flat_map(|c| c.to_str())
                .any(|c| c == "keep-alive")
            {
                ConnectionState::KeepAlive
            } else {
                ConnectionState::Close
            };

            // let content_length: Option<usize> = response
            //     .headers
            //     .get(CONTENT_LENGTH)
            //     .iter()
            //     .flat_map(|c| c.to_str())
            //     .flat_map(|s| s.parse())
            //     .next();

            // dbg!(content_length);

            log::debug!(
                "successfull readed reponse, status {}, {} bytes, {} headers, connection: {:?}, duration: {}ms",
                response.status,
                response.body.len(),
                response.headers.len(),
                connection_state,
                response.duration.as_millis(),
            );

            if let Some(c) = response
                .headers
                .get(HeaderName::from_bytes(b"Keep-Alive").unwrap())
            {
                log::warn!("KEEP_ALIVE: {:?}", c);
            }

            return Ok((response, ConnectionState::KeepAlive));
        }
        loops += 1;
    }
}

struct Callback {
    error: Option<Error>,
    body: Vec<u8>,
    headers: HeaderMap,
    last_header_name: Option<HeaderName>,
    status: Option<StatusCode>,
    is_message_complete: bool,
    started_at: Instant,
}

impl Callback {
    fn to_response(mut self, _reponse_body_limit: usize) -> Result<Response> {
        match self.status {
            Some(status) => {
                let mut response = Response {
                    status,
                    headers: HeaderMap::with_capacity(0),
                    body: Vec::with_capacity(0),
                    duration: Instant::now().duration_since(self.started_at),
                };

                std::mem::swap(&mut response.headers, &mut self.headers);

                if self.body.len() > 0 {
                    match response
                        .headers
                        .get(CONTENT_ENCODING)
                        .iter()
                        .flat_map(|v| v.to_str().ok())
                        .next()
                    {
                        None | Some("identity") => {
                            std::mem::swap(&mut response.body, &mut self.body);
                        }
                        Some("gzip") => {
                            let err = |err| Error {
                                text: format!(
                                    "could not uncompress gzip compressed response body: {}",
                                    err,
                                ),
                            };
                            let mut decoder = Decoder::new(&self.body[..]).map_err(err)?;
                            io::copy(&mut decoder, &mut response.body).map_err(err)?;
                            log::debug!(
                                "decompressing: {} ==> {}",
                                self.body.len(),
                                response.body.len()
                            );
                        }
                        Some(encoding) => {
                            return Err(Error {
                                text: format!("got unexpected encoding '{}'", encoding),
                            })
                        }
                    }
                }

                Ok(response)
            }
            None => Err(Error {
                text: format!("illformed header"),
            }),
        }
    }
}

impl HttpParserCallback for Callback {
    fn on_message_begin(&mut self, _parser: &mut HttpParser) -> CallbackResult {
        Ok(http_parser::ParseAction::None)
    }

    fn on_status(&mut self, parser: &mut HttpParser, _data: &[u8]) -> CallbackResult {
        match parser
            .status_code
            .and_then(|s| StatusCode::from_u16(s).ok())
        {
            Some(status) => self.status = Some(status),
            None => {
                self.error = Some(Error {
                    text: format!("illformed reponse"),
                })
            }
        }

        Ok(http_parser::ParseAction::None)
    }

    fn on_header_field(&mut self, _parser: &mut HttpParser, data: &[u8]) -> CallbackResult {
        match HeaderName::from_bytes(data) {
            Ok(n) => self.last_header_name = Some(n),
            Err(_) => {
                self.error = Some(Error {
                    text: format!("illformed header"),
                })
            }
        }
        Ok(ParseAction::None)
    }

    fn on_header_value(&mut self, _parser: &mut HttpParser, data: &[u8]) -> CallbackResult {
        match (
            // TODO: could we take ownership here?
            self.last_header_name.clone(),
            String::from_utf8(data.to_vec())
                .map_err(|_| ())
                .and_then(|v| v.parse::<HeaderValue>().map_err(|_| ())),
        ) {
            (Some(n), Ok(v)) => {
                self.headers.insert(n, v);
                self.last_header_name = None;
            }
            _ => {
                self.error = Some(Error {
                    text: format!("illformed header"),
                })
            }
        }
        Ok(ParseAction::None)
    }

    fn on_headers_complete(&mut self, parser: &mut HttpParser) -> CallbackResult {
        if self.status.is_none() {
            self.status = parser.status_code.iter().flat_map(|s| StatusCode::from_u16(*s)).next();
        }
        Ok(ParseAction::None)
    }

    fn on_body(&mut self, _parser: &mut HttpParser, data: &[u8]) -> CallbackResult {
        self.body.extend(data);
        Ok(ParseAction::None)
    }

    fn on_message_complete(&mut self, _parser: &mut HttpParser) -> CallbackResult {
        self.is_message_complete = true;
        Ok(ParseAction::None)
    }
}
