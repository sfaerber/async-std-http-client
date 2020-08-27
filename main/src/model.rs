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
}

pub struct RequestBuilder {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) body: Option<Vec<u8>>,
    pub(crate) headers: http::HeaderMap,
}

pub trait ToPath {
    fn to_path(self) -> String;
}

impl ToPath for String {
    fn to_path(self) -> String {
        self
    }
}

impl ToPath for &str {
    fn to_path(self) -> String {
        self.to_string()
    }
}

impl ToPath for &&str {
    fn to_path(self) -> String {
        self.to_string()
    }
}

impl Request {
    pub fn get<P>(path: P) -> RequestBuilder
    where
        P: ToPath,
    {
        Self::build(Method::Get, path)
    }

    pub fn post<P>(path: P) -> RequestBuilder
    where
        P: ToPath,
    {
        Self::build(Method::Post, path)
    }

    pub fn put<P>(path: P) -> RequestBuilder
    where
        P: ToPath,
    {
        Self::build(Method::Put, path)
    }

    pub fn delete<P>(path: P) -> RequestBuilder
    where
        P: ToPath,
    {
        Self::build(Method::Get, path)
    }

    pub fn build<P>(method: Method, path: P) -> RequestBuilder
    where
        P: ToPath,
    {
        RequestBuilder {
            method,
            path: path.to_path(),
            body: None,
            headers: http::HeaderMap::new(),
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

    pub fn build(&mut self) -> Request {
        //
        let mut result = Request {
            method: self.method,
            path: String::new(),
            body: None,
            headers: HeaderMap::new(),
        };

        std::mem::swap(&mut result.path, &mut self.path);
        std::mem::swap(&mut result.body, &mut self.body);
        std::mem::swap(&mut result.headers, &mut self.headers);

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

#[derive(Debug)]
pub struct ClientConfig {
    pub use_tls: bool,
    pub host: String,
    pub port: u16,
    pub url_prefix: String,
    pub connection_idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_connections: usize,
    pub request_timeout: Duration,
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
