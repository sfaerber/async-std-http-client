mod model;
mod request_writer;
mod response_parser;

use crate::request_writer::write_request;
use crate::response_parser::read_reponse;
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
pub use crate::model::{Method, Request, Response};

pub use http::header::*;
pub use http::status::*;
use task::Waker;

use futures::future::FutureExt;
use std::sync::Mutex;

#[derive(Clone)]
pub struct Builder {
    base_url: String,
    max_connections: usize,
    connection_idle_timeout: Duration,
    connect_timeout: Duration,
}

impl Builder {
    pub fn connection_count(&mut self, max_connections: usize) -> &mut Self {
        self.max_connections = max_connections;
        self
    }

    pub fn connection_idle_timeout(&mut self, connection_idle_timeout: Duration) -> &mut Self {
        self.connection_idle_timeout = connection_idle_timeout;
        self
    }

    pub fn build(self) -> Result<Client> {
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
            url_prefix: url.path().to_string(),
            connection_idle_timeout: self.connection_idle_timeout,
            connect_timeout: self.connect_timeout,
            max_connections: self.max_connections,
        };

        let state = ClientState {
            idle_connections: im::OrdMap::new(),
            connecting_connections: im::OrdSet::new(),
            requests: im::OrdMap::new(),
            connection_counter: 0,
            request_counter: 0,
        };

        log::info!("builded client wiht config: {:?}", &config);

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
    idle_connections: im::OrdMap<ConnectionId, (Option<Waker>, ConnectionBusiness)>,
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
        ),
    >,
    request_counter: u64,
}

impl ClientState {
    fn connection_count(&self) -> usize {
        self.idle_connections.len() + self.connecting_connections.len()
    }

    fn longest_waiting_request_id(&self) -> Option<RequestId> {
        self.requests
            .iter()
            .rev()
            .filter(|(_, (_, _, _, _, c))| c.is_none())
            .map(|(id, _)| *id)
            .next()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct Client {
    state: Arc<ArcSwap<ClientState>>,
    config: Arc<ClientConfig>,
}

impl Client {
    fn _print_state(&self) -> String {
        use itertools::*;
        let state = self.state.load();
        format!(
            "idle: [{}], working: [{}], connecting: [{}], waiting: [{}]",
            state
                .idle_connections
                .iter()
                .filter(|(_, (_, b))| b == &ConnectionBusiness::Idle)
                .map(|i| i.0)
                .join(","),
            state
                .idle_connections
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
            base_url: base_url.to_string(),
            max_connections: 10,
            connection_idle_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
        }
    }

    async fn connect(&self) -> Result<Box<dyn AsyncReadWriter>> {
        //
        let config = &self.config;

        let tcp_stream = TcpStream::connect(format!("{}:{}", config.host, config.port))
            .await
            .map_err(|err| Error {
                text: format!("could not etablish TCP connection{}", err),
            })?;

        log::debug!("tcp stream to '{}:{}' created", config.host, config.port);

        if config.use_tls {
            let connector = TlsConnector::default();

            log::debug!("opening tsl stream to server name '{}'", &config.host);

            let tls_stream: TlsStream<_> = connector
                .connect(&config.host, tcp_stream)
                .await
                .map_err(|err| Error {
                    text: format!("could not etablish TLS connection: {}", err),
                })?;

            Ok(Box::new(tls_stream))
        } else {
            Ok(Box::new(tcp_stream))
        }
    }

    async fn req_internal(
        &self,
        con: &mut Connection,
        req: &Request,
    ) -> Result<(Response, ConnectionState)> {
        write_request(con, &req, &self.config).await?;
        read_reponse(con, &self.config).await
    }

    async fn add_connection_if_undeflow(&self) -> Result<()> {
        let max_connections = self.config.max_connections;

        let old_state = self.state.rcu(move |s| {
            let mut s = (**s).clone();
            if s.connection_count() < max_connections && s.idle_connections.len() == 0 {
                s.connection_counter += 1;
                s.connecting_connections
                    .insert(ConnectionId(s.connection_counter));
            }
            s
        });

        if old_state.connection_count() < max_connections && old_state.idle_connections.len() == 0 {
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
                        if s.idle_connections
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

    pub async fn req(&self, req: Request) -> Result<Response> {
        self.add_connection_if_undeflow().await?;

        let req = Arc::new(req);

        let request_id = RequestId(
            self.state
                .rcu(move |s| {
                    let mut s = (**s).clone();
                    s.request_counter += 1;
                    if s.requests
                        .insert(
                            RequestId(s.request_counter),
                            (Instant::now(), req.clone(), None, None, None),
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

        for (_, (waker_option, _)) in self
            .state
            .load()
            .idle_connections
            .iter()
            .filter(|(_, (_, x))| x == &ConnectionBusiness::Idle)
        {
            if let Some(waker) = waker_option {
                waker.wake_by_ref();
            }
        }

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

    async fn run_connection(&self, mut con: Connection) {
        let connection_id = con.0;
        loop {
            let mut aw = AllocateWorkFuture {
                connection_id,
                client: &self,
            }
            .fuse();

            let mut te = ConnectionTerminationFuture {
                con: &mut con,
                started_at: Instant::now(),
                timeout: self.config.connection_idle_timeout,
            }
            .fuse();

            let waiting_id_option: Option<(Arc<Request>, RequestId)> = futures::select! {
              i = aw => Some(i),
              _ = te => None,
            };

            ////////////////////////////////////////////////////////////

            if let Some((req, request_id)) = waiting_id_option {
                log::debug!("doing {} in {}", request_id, connection_id);

                self.state.rcu(move |s| {
                    let mut s = (**s).clone();
                    if let Some(tpl) = s.idle_connections.get_mut(&connection_id) {
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
                            if s.idle_connections.remove(&connection_id).is_none() {
                                illegal_state(20)
                            }
                        }
                        ConnectionState::KeepAlive => {
                            if let Some(tpl) = s.idle_connections.get_mut(&connection_id) {
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
                        break;
                    }
                    ConnectionState::KeepAlive => {
                        // go ahead..
                    }
                }
            } else {
                self.state.rcu(move |s| {
                    let mut s = (**s).clone();
                    if s.idle_connections.remove(&connection_id).is_none() {
                        illegal_state(30)
                    }
                    s
                });

                log::debug!("closing connection/2 {}", connection_id);
                drop(con);
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
                task::Poll::Pending => "Pending".to_string(),
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
    type Output = (Arc<Request>, RequestId);
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
            if let Some(w) = s.idle_connections.get_mut(&connection_id) {
                w.0 = Some(cx.waker().clone());
            } else {
                illegal_state(16)
            }
            s
        });

        let id_option = old_state.longest_waiting_request_id();
        if let Some(id) = id_option {
            if let Some(tpl) = old_state.requests.get(&id) {
                task::Poll::Ready((tpl.1.clone(), id))
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
    started_at: Instant,
    timeout: Duration,
}

impl<'a> ConnectionTerminationFuture<'a> {
    fn timed_out(&self) -> bool {
        Instant::now() > self.started_at + self.timeout
    }
}

impl<'a> Future for ConnectionTerminationFuture<'a> {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Self::Output> {
        use task::Poll::*;
        let timed_out = self.timed_out();
        let mut buffer: [u8; 4096] = [0; 4096];
        match Pin::new(&mut self.get_mut().con.1).poll_read(cx, &mut buffer) {
            Ready(len) => {
                match len {
                    Ok(0) => {}
                    len => log::warn!("connection drop: {:?}", len),
                }
                Ready(())
            }
            Pending if timed_out => Ready(()),
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
