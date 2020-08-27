use encoding::EncodingRef;
use futures::{AsyncRead, AsyncWrite};
use http::{
    header::{HeaderName, AUTHORIZATION},
    status::StatusCode,
    HeaderMap, HeaderValue,
};
use std::time::Duration;

pub type Result<T> = std::result::Result<T, Error>;

pub enum InternalRequestError {
    ConnectionIsClosed,
    UnrecoverableError(Error),
}

pub type InternalRequestResult<T> = std::result::Result<T, InternalRequestError>;

pub trait AsyncReadWriter: 'static + Unpin + Send + Sync + AsyncRead + AsyncWrite {}

impl<T> AsyncReadWriter for T where T: 'static + Unpin + Send + Sync + AsyncRead + AsyncWrite {}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Debug)]
pub struct ConnectionId(pub u64);

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "C{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Debug)]
pub struct WaitingId(pub u64);

impl std::fmt::Display for WaitingId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "W{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Debug)]
pub struct RequestId(pub u64);

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "R{}", self.0)
    }
}

pub type Connection = (ConnectionId, Box<dyn AsyncReadWriter>);

#[derive(Clone, Debug)]
pub struct Error {
    pub text: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.text)
    }
}
pub struct Request {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) headers: http::HeaderMap,
    pub(crate) request_args: Vec<(String, String)>,
}

pub struct RequestBuilder {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) headers: http::HeaderMap,
    pub(crate) request_args: Vec<(String, String)>,
}

pub trait ToRequestPartString {
    fn to_owned_string(self) -> String;
}

impl ToRequestPartString for String {
    fn to_owned_string(self) -> String {
        self
    }
}

impl ToRequestPartString for &str {
    fn to_owned_string(self) -> String {
        self.to_string()
    }
}

impl ToRequestPartString for &&str {
    fn to_owned_string(self) -> String {
        self.to_string()
    }
}

impl Request {
    pub fn get<P>(path: P) -> RequestBuilder
    where
        P: ToRequestPartString,
    {
        Self::build(Method::Get, path)
    }

    pub fn post<P>(path: P) -> RequestBuilder
    where
        P: ToRequestPartString,
    {
        Self::build(Method::Post, path)
    }

    pub fn put<P>(path: P) -> RequestBuilder
    where
        P: ToRequestPartString,
    {
        Self::build(Method::Put, path)
    }

    pub fn delete<P>(path: P) -> RequestBuilder
    where
        P: ToRequestPartString,
    {
        Self::build(Method::Get, path)
    }

    pub fn build<P>(method: Method, path: P) -> RequestBuilder
    where
        P: ToRequestPartString,
    {
        RequestBuilder {
            method,
            path: path.to_owned_string(),
            body: None,
            headers: http::HeaderMap::new(),
            request_args: Vec::new(),
        }
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn body(&self) -> &Option<Vec<u8>> {
        &self.body
    }

    pub fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }
}

impl RequestBuilder {
    pub fn with_body(&mut self, body: Vec<u8>) -> &mut Self {
        self.body = Some(body);
        self
    }

    pub fn with_header(&mut self, name: HeaderName, value: HeaderValue) -> &mut Self {
        self.headers.insert(name, value);
        self
    }

    pub fn with_basic_auth(&mut self, username: &str, password: &str) -> &mut Self {
        let payload = base64::encode(format!("{}:{}", username, password));
        let payload = format!("Basic {}", payload);
        self.headers.insert(AUTHORIZATION, payload.parse().unwrap());
        self
    }

    pub fn with_request_args<N, V, I: IntoIterator<Item = (N, V)>>(&mut self, args: I) -> &mut Self
    where
        N: ToRequestPartString,
        V: ToRequestPartString,
    {
        for (n, v) in args.into_iter() {
            self.with_request_arg(n, v);
        }
        self
    }

    pub fn with_request_arg<N, V>(&mut self, name: N, value: V) -> &mut Self
    where
        N: ToRequestPartString,
        V: ToRequestPartString,
    {
        self.request_args
            .push((name.to_owned_string(), value.to_owned_string()));
        self
    }

    pub fn build(&mut self) -> Request {
        //
        let mut result = Request {
            method: self.method,
            path: String::new(),
            body: None,
            headers: HeaderMap::new(),
            request_args: Vec::new(),
        };

        std::mem::swap(&mut result.path, &mut self.path);
        std::mem::swap(&mut result.body, &mut self.body);
        std::mem::swap(&mut result.headers, &mut self.headers);
        std::mem::swap(&mut result.request_args, &mut self.request_args);

        result
    }
}

pub struct Response {
    pub status: StatusCode,
    pub body: Vec<u8>,
    pub headers: http::HeaderMap,
    pub duration: Duration,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    //Options,
    Get,
    Post,
    Put,
    Delete,
    //Head,
    //Trace,
    //Connect,
    //Patch,
}

impl Method {
    pub fn to_str(&self) -> &'static str {
        use Method::*;
        match self {
            Get => "GET",
            Post => "POST",
            Delete => "DELETE",
            Put => "PUT",
        }
    }
}

// pub enum Body {
//     Text(String),
//     Json(serde_json::value::Value),
//     Protobuf(Box<dyn prost::Message>),
// }

pub struct ClientConfig {
    pub use_tls: bool,
    pub host: String,
    pub port: u16,
    pub url_prefix: String,
    pub connection_idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_connections: usize,
    pub request_timeout: Duration,
    pub url_encoding: EncodingRef,
    pub response_body_limit: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum ConnectionState {
    Close,
    KeepAlive,
}

// #[derive(Debug, Clone, Copy)]
// pub enum ConnectionCloseReason {
//     HttpHeaderNoKeepAlive,
//     DroppedWhileIdle,
//     ErrorDuringRequest,
// }
