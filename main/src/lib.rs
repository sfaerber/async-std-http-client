mod model;
mod request_writer;
mod response_parser;

use crate::request_writer::write_request;
use crate::response_parser::read_response;
use arc_swap::ArcSwap;
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::task;
use async_tls::{client::TlsStream, TlsConnector};
use std::sync::Arc;
use std::time::Instant;
use std::{pin::Pin, time::Duration};
use url::Url;

use crate::model::*;
pub use crate::model::{Method, Request, RequestBuilder, Response};

pub use http::header::*;
pub use http::status::*;
use task::Waker;

use encoding::EncodingRef;
use futures::future::FutureExt;
use std::sync::Mutex;

#[derive(Clone)]
pub struct Builder {
    base_url: String,
    max_connections: usize,
    connection_idle_timeout: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
    url_encoding: EncodingRef,
    leave_content_encoded: bool,
    do_url_encoding: bool,
    accept_invalid_cert: bool,
}

impl Builder {
    pub fn max_connections(&mut self, max_connections: usize) -> &mut Self {
        self.max_connections = max_connections;
        self
    }

    pub fn connection_idle_timeout(&mut self, connection_idle_timeout: Duration) -> &mut Self {
        self.connection_idle_timeout = connection_idle_timeout;
        self
    }

    pub fn request_timeout(&mut self, request_timeout: Duration) -> &mut Self {
        self.request_timeout = request_timeout;
        self
    }

    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Self {
        self.connect_timeout = connect_timeout;
        self
    }

    pub fn url_encoding(&mut self, url_encoding: EncodingRef) -> &mut Self {
        self.url_encoding = url_encoding;
        self
    }

    pub fn leave_content_encoded(&mut self, leave_content_encoded: bool) -> &mut Self {
        self.leave_content_encoded = leave_content_encoded;
        self
    }

    pub fn do_url_encoding(&mut self, do_url_encoding: bool) -> &mut Self {
        self.do_url_encoding = do_url_encoding;
        self
    }

    pub fn accept_invalid_cert(&mut self, accept_invalid_cert: bool) -> &mut Self {
        self.accept_invalid_cert = accept_invalid_cert;
        self
    }

    pub fn build(&self) -> Result<Client> {
        //
        let url = Url::parse(&self.base_url).map_err(|err| Error {
            text: format!("could not parse base url: {}", err),
        })?;

        let use_tls = match url.scheme() {
            "https" => true,
            "http" => false,
            scheme => {
                return Err(Error {
                    text: format!("expected scheme to be 'http' or 'https', got '{}'", scheme),
                })
            }
        };

        let host = match url.host() {
            None => {
                return Err(Error {
                    text: format!("base url must have a host"),
                })
            }
            // TODO: do we need to prevent ip hosts for TSL connections?
            Some(host) => host.to_string(),
        };

        let port = match url.port() {
            Some(port) => port,
            None => {
                if use_tls {
                    443
                } else {
                    80
                }
            }
        };

        let config = ClientConfig {
            use_tls,
            host,
            port,
            url_prefix: url.path().to_owned_string(),
            connection_idle_timeout: self.connection_idle_timeout,
            connect_timeout: self.connect_timeout,
            max_connections: self.max_connections,
            request_timeout: self.request_timeout,
            url_encoding: self.url_encoding,
            response_body_limit: 1024 * 1024 * 100, // limit of 100MB for now
            leave_content_encoded: self.leave_content_encoded,
            do_url_encoding: self.do_url_encoding,
            accept_invalid_cert: self.accept_invalid_cert,
        };

        let state = ClientState {
            living_connections: im::OrdMap::new(),
            connecting_connections: im::OrdSet::new(),
            requests: im::OrdMap::new(),
            connection_counter: 0,
            request_counter: 0,
        };

        //log::info!("builded client wiht config: {:?}", &config);

        Ok(Client {
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            config: Arc::new(config),
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Debug)]
enum ConnectionBusiness {
    Idle,
    Working,
}

#[derive(Clone)]
struct ClientState {
    living_connections: im::OrdMap<ConnectionId, (Option<Waker>, ConnectionBusiness)>,
    connecting_connections: im::OrdSet<ConnectionId>,
    connection_counter: u64,
    requests: im::OrdMap<
        RequestId,
        (
            Instant,
            Arc<Request>,
            Option<Arc<Mutex<Option<Result<Response>>>>>,
            Option<Waker>,
            Option<ConnectionId>,
            i16,
        ),
    >,
    request_counter: u64,
}

impl ClientState {
    fn connection_count(&self) -> usize {
        self.living_connections.len() + self.connecting_connections.len()
    }

    fn longest_waiting_request_id(&self) -> Option<RequestId> {
        self.requests
            .iter()
            .rev()
            .filter(|(_, (_, _, _, _, c, _))| c.is_none())
            .map(|(id, _)| *id)
            .next()
    }

    fn needs_more_connections(&self, config: &ClientConfig) -> bool {
        self.connection_count() < config.max_connections
            && self.connection_count() < self.requests.len()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct Client {
    state: Arc<ArcSwap<ClientState>>,
    config: Arc<ClientConfig>,
}

const MAX_REPETITIONS: i16 = 5;

impl Client {
    pub fn print_internal_state(&self) -> String {
        use itertools::*;
        let state = self.state.load();
        format!(
            "idle: [{}], working: [{}], connecting: [{}], waiting: [{}]",
            state
                .living_connections
                .iter()
                .filter(|(_, (_, b))| b == &ConnectionBusiness::Idle)
                .map(|i| i.0)
                .join(","),
            state
                .living_connections
                .iter()
                .filter(|(_, (_, b))| b == &ConnectionBusiness::Working)
                .map(|i| i.0)
                .join(","),
            state.connecting_connections.iter().map(|i| i.0).join(","),
            state.requests.iter().map(|i| i.0).join(","),
        )
    }

    pub fn builder(base_url: &str) -> Builder {
        Builder {
            base_url: base_url.to_owned_string(),
            max_connections: 10,
            connection_idle_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
            url_encoding: encoding::all::UTF_8,
            leave_content_encoded: false,
            do_url_encoding: true,
            accept_invalid_cert: false,
        }
    }

    async fn connect_wihtout_timeout(&self) -> Result<Box<dyn AsyncReadWriter>> {
        //
        let config = &self.config;

        let tcp_stream = TcpStream::connect(format!("{}:{}", config.host, config.port))
            .await
            .map_err(|err| Error {
                text: format!("could not etablish TCP connection{}", err),
            })?;

        log::debug!("tcp stream to '{}:{}' created", config.host, config.port);

        if config.use_tls {
            let connector = if self.config.accept_invalid_cert {
                let mut config = rustls::ClientConfig::new();
                config
                    .dangerous()
                    .set_certificate_verifier(Arc::new(danger::NoCertificateVerification {}));

                TlsConnector::from(config)
            } else {
                TlsConnector::default()
            };

            log::debug!("opening tsl stream to server name '{}'", &config.host);

            let tls_stream: TlsStream<_> = connector
                .connect(&config.host, tcp_stream)
                .await
                .map_err(|err| Error {
                    text: format!(
                        "could not etablish TLS connection [accept_invalid_cert={}]: {}",
                        self.config.accept_invalid_cert,
                        err
                    ),
                })?;

            Ok(Box::new(tls_stream))
        } else {
            Ok(Box::new(tcp_stream))
        }
    }

    async fn connect(&self) -> Result<Box<dyn AsyncReadWriter>> {
        futures::select! {
            r = self.connect_wihtout_timeout().fuse() => r,
            _ = async_std::task::sleep(self.config.connect_timeout).fuse() =>
                Err(Error {
                    text: format!("connect timed out after {:?}", self.config.connect_timeout),
                }),
        }
    }

    async fn req_internal(
        &self,
        con: &mut Connection,
        req: &Request,
    ) -> InternalRequestResult<(Response, ConnectionState)> {
        futures::select! {
            r = async {
                let start = Instant::now();
                write_request(con, &req, &self.config).await?;
                read_response(con, &self.config, start).await
            }.fuse() => r,
            _ = async_std::task::sleep(self.config.request_timeout).fuse() =>
                Err(InternalRequestError::UnrecoverableError(Error {
                    text: format!("request timed out after {:?}", self.config.request_timeout),
                })),
        }
    }

    async fn add_connection_if_undeflow(&self) -> Result<()> {
        let old_state = self.state.rcu(move |s| {
            let mut s = (**s).clone();
            if s.needs_more_connections(&self.config) {
                s.connection_counter += 1;
                s.connecting_connections
                    .insert(ConnectionId(s.connection_counter));
            }
            s
        });

        if old_state.needs_more_connections(&self.config) {
            let connection_id = ConnectionId(old_state.connection_counter + 1);
            match self.connect().await {
                Ok(con) => {
                    let con = (connection_id, con);
                    log::debug!("created new connection {}", con.0);
                    self.state.rcu(move |s| {
                        let mut s = (**s).clone();
                        if s.connecting_connections.remove(&connection_id).is_none() {
                            illegal_state(1)
                        }
                        if s.living_connections
                            .insert(connection_id, (None, ConnectionBusiness::Idle))
                            .is_some()
                        {
                            illegal_state(2)
                        }
                        s
                    });
                    self.spawn_connection(con);
                }
                Err(err) => {
                    self.state.rcu(move |s| {
                        let mut s = (**s).clone();
                        if s.connecting_connections.remove(&connection_id).is_none() {
                            illegal_state(3)
                        }
                        s
                    });
                    log::error!("could not create connection: {}", err);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn wake_connection(&self) {
        for waker in self
            .state
            .load()
            .living_connections
            .iter()
            .filter(|(_, (_, x))| x == &ConnectionBusiness::Idle)
            .flat_map(|(_, (waker_option, _))| waker_option.iter())
            .take(2)
        {
            waker.wake_by_ref();
        }
    }

    pub async fn req(&self, req: Request) -> Result<Response> {
        let req = Arc::new(req);

        let request_id = RequestId(
            self.state
                .rcu(move |s| {
                    let mut s = (**s).clone();
                    s.request_counter += 1;
                    if s.requests
                        .insert(
                            RequestId(s.request_counter),
                            (Instant::now(), req.clone(), None, None, None, -1),
                        )
                        .is_some()
                    {
                        illegal_state(5)
                    };
                    s
                })
                .request_counter
                + 1,
        );

        log::debug!("request {} is scheduled", request_id);

        self.add_connection_if_undeflow().await?;

        self.wake_connection();

        ResponseFuture {
            request_id,
            client: &self,
        }
            .await
    }

    fn spawn_connection(&self, con: Connection) {
        let client = self.clone();
        task::spawn(async move {
            client.run_connection(con).await;
        });
    }

    async fn reopen_connections(&self) {
        match self.add_connection_if_undeflow().await {
            Ok(()) => {
                self.wake_connection();
            }
            Err(err) => {
                self.state.rcu(move |s| {
                    let mut s = (**s).clone();
                    let kill_requests: Vec<RequestId> = s
                        .requests
                        .iter()
                        .filter(|(_, tpl)| tpl.4.is_none())
                        .map(|(id, _)| *id)
                        .collect();

                    for request_id in kill_requests {
                        if let Some(tpl) = s.requests.get_mut(&request_id) {
                            tpl.2 = Some(Arc::new(Mutex::new(Some(Err(err.clone())))));
                        }
                    }

                    s
                });
            }
        }
    }

    async fn run_connection(&self, mut con: Connection) {
        let connection_id = con.0;
        loop {
            let direct_waiting_id_option: Option<(Arc<Request>, RequestId, bool)> = {
                let old_state = self.state.rcu(move |s| {
                    let mut s = (**s).clone();
                    let request_id = s.longest_waiting_request_id();
                    if let Some(id) = request_id {
                        if let Some(tpl) = s.requests.get_mut(&id) {
                            tpl.4 = Some(connection_id)
                        } else {
                            illegal_state(45)
                        }
                    }
                    s
                });

                let id_option = old_state.longest_waiting_request_id();
                if let Some(id) = id_option {
                    if let Some(tpl) = old_state.requests.get(&id) {
                        Some((tpl.1.clone(), id, tpl.5 < MAX_REPETITIONS))
                    } else {
                        illegal_state(47)
                    }
                } else {
                    None
                }
            };

            ////////////////////////////////////////

            let waiting_id_option: Option<(Arc<Request>, RequestId, bool)> = {
                if let Some(r) = direct_waiting_id_option {
                    Some(r)
                } else {
                    let mut aw = AllocateWorkFuture {
                        connection_id,
                        client: &self,
                    }
                        .fuse();

                    futures::select! {
                      i = aw => Some(i),
                      _ = ConnectionTerminationFuture { con: &mut con }.fuse() => None,
                      _ = async_std::task::sleep(self.config.connection_idle_timeout).fuse() => None,
                    }
                }
            };

            ////////////////////////////////////////////////////////////

            if let Some((req, request_id, repeat_allowed)) = waiting_id_option {
                log::debug!("doing {} in {}", request_id, connection_id);

                self.state.rcu(move |s| {
                    let mut s = (**s).clone();
                    if let Some(tpl) = s.living_connections.get_mut(&connection_id) {
                        if tpl.1 != ConnectionBusiness::Idle {
                            illegal_state(23)
                        }
                        tpl.1 = ConnectionBusiness::Working;
                    } else {
                        illegal_state(24)
                    }
                    s
                });

                let (result, cs) = match self.req_internal(&mut con, &req).await {
                    Ok((r, s)) => (Ok(r), s),
                    Err(err) => (Err(err), ConnectionState::Close),
                };

                let result: Result<Response> = match result {
                    Ok(tpl) => Ok(tpl),
                    Err(InternalRequestError::UnrecoverableError(err)) => Err(err),
                    Err(InternalRequestError::ConnectionIsClosed) if repeat_allowed => {
                        log::debug!("remote connection was closed during request, recovering..");

                        self.state.rcu(move |s| {
                            let mut s = (**s).clone();
                            if let Some(tpl) = s.requests.get_mut(&request_id) {
                                tpl.4 = None;
                                tpl.5 += 1; // increment retry counter
                            } else {
                                illegal_state(30)
                            }
                            if s.living_connections.remove(&connection_id).is_none() {
                                illegal_state(31)
                            }
                            s
                        });
                        self.reopen_connections().await;
                        break;
                    }
                    Err(InternalRequestError::ConnectionIsClosed) =>
                        Err(Error { text : format!(
                            "the underlying connection was unexpectedly closed and the maximum retry count of {} was exceeded",
                            MAX_REPETITIONS)
                        })
                };

                let result = Arc::new(Mutex::new(Some(result)));

                let old_state = self.state.rcu(move |s| {
                    let mut s = (**s).clone();
                    if let Some(tpl) = s.requests.get_mut(&request_id) {
                        tpl.2 = Some(result.clone());
                    } else {
                        illegal_state(15)
                    }
                    match cs {
                        ConnectionState::Close => {
                            if s.living_connections.remove(&connection_id).is_none() {
                                illegal_state(20)
                            }
                        }
                        ConnectionState::KeepAlive => {
                            if let Some(tpl) = s.living_connections.get_mut(&connection_id) {
                                if tpl.1 != ConnectionBusiness::Working {
                                    illegal_state(22)
                                }
                                tpl.1 = ConnectionBusiness::Idle;
                            } else {
                                illegal_state(21)
                            }
                        }
                    }
                    s
                });

                if let Some(tpl) = &old_state.requests.get(&request_id) {
                    if let Some(waker) = &tpl.3 {
                        waker.wake_by_ref();
                    }
                } else {
                    illegal_state(16)
                }

                match cs {
                    ConnectionState::Close => {
                        log::debug!("closing connection/1 {}", connection_id);
                        drop(con);
                        self.reopen_connections().await;
                        break;
                    }
                    ConnectionState::KeepAlive => {
                        // go ahead..
                    }
                }
            } else {
                self.state.rcu(move |s| {
                    let mut s = (**s).clone();
                    if s.living_connections.remove(&connection_id).is_none() {
                        illegal_state(30)
                    }
                    s
                });

                log::debug!("closing connection/2 {}", connection_id);
                drop(con);
                self.reopen_connections().await;
                break;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

struct ResponseFuture<'a> {
    request_id: RequestId,
    client: &'a Client,
}

impl<'a> Future for ResponseFuture<'a> {
    type Output = Result<Response>;
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        //
        let request_id = self.request_id;

        let old_state = self.client.state.rcu(move |s| {
            let mut s = (**s).clone();
            let mut done = false;
            if let Some(tpl) = s.requests.get_mut(&request_id) {
                if tpl.2.is_some() {
                    done = true;
                } else {
                    tpl.3 = Some(cx.waker().clone())
                }
            } else {
                illegal_state(10)
            };
            if done {
                if s.requests.remove(&request_id).is_none() {
                    illegal_state(11)
                }
            }
            s
        });

        let result = if let Some(tpl) = old_state.requests.get(&request_id) {
            match &tpl.2 {
                Some(result_ptr_option) => {
                    let mut result_option: Option<Result<Response>> = None;
                    let mut lock = match result_ptr_option.try_lock() {
                        Ok(l) => l,
                        Err(_) => illegal_state(8),
                    };
                    std::mem::swap(&mut result_option, &mut lock);
                    drop(lock);
                    match result_option {
                        Some(result) => task::Poll::Ready(result),
                        None => illegal_state(9),
                    }
                }
                None => task::Poll::Pending,
            }
        } else {
            illegal_state(12)
        };

        log::debug!(
            "result for {} polled, {}",
            request_id,
            match &result {
                task::Poll::Ready(Ok(result)) =>
                    format!("got {}, {} bytes ", result.status, result.body.len()),
                task::Poll::Ready(Err(err)) => format!("got error: {}", err),
                task::Poll::Pending => "Pending".to_owned_string(),
            }
        );

        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

struct AllocateWorkFuture<'a> {
    connection_id: ConnectionId,
    client: &'a Client,
}

impl<'a> Future for AllocateWorkFuture<'a> {
    type Output = (Arc<Request>, RequestId, bool);
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let connection_id = self.connection_id;
        let old_state = self.client.state.rcu(move |s| {
            let mut s = (**s).clone();
            let request_id = s.longest_waiting_request_id();
            if let Some(id) = request_id {
                if let Some(tpl) = s.requests.get_mut(&id) {
                    tpl.4 = Some(connection_id)
                } else {
                    illegal_state(15)
                }
            }
            if let Some(w) = s.living_connections.get_mut(&connection_id) {
                w.0 = Some(cx.waker().clone());
            } else {
                illegal_state(16)
            }
            s
        });

        let id_option = old_state.longest_waiting_request_id();
        if let Some(id) = id_option {
            if let Some(tpl) = old_state.requests.get(&id) {
                task::Poll::Ready((tpl.1.clone(), id, tpl.5 < MAX_REPETITIONS))
            } else {
                illegal_state(17)
            }
        } else {
            task::Poll::Pending
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

struct ConnectionTerminationFuture<'a> {
    con: &'a mut Connection,
}

impl<'a> Future for ConnectionTerminationFuture<'a> {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Self::Output> {
        use task::Poll::*;
        let mut buffer: [u8; 4096] = [0; 4096];
        match Pin::new(&mut self.get_mut().con.1).poll_read(cx, &mut buffer) {
            Ready(len) => {
                match len {
                    Ok(0) => {}
                    len => log::warn!("connection drop: {:?}", len),
                }
                Ready(())
            }
            Pending => Pending,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

fn illegal_state<T>(errno: usize) -> T {
    log::error!(
        "illegal state in http client connection management, this is a bug, error={}",
        errno
    );
    panic!(
        "illegal state in http client connection management, this is a bug, error={}",
        errno
    )
}

mod danger {
    use webpki;

    pub struct NoCertificateVerification {}

    impl rustls::ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _roots: &rustls::RootCertStore,
            _presented_certs: &[rustls::Certificate],
            _dns_name: webpki::DNSNameRef<'_>,
            _ocsp: &[u8],
        ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
            log::debug!("building tls connection without cert verification");
            Ok(rustls::ServerCertVerified::assertion())
        }
    }
}
