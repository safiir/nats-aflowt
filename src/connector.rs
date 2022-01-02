// Copyright 2020-2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{self, Error, ErrorKind};
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use url::Url;

//use tokio_rustls::webpki::DnsNameRef;

use crate::auth_utils;
use crate::proto::{self, ClientOp, ServerOp};
use crate::rustls::{ClientConfig, /* ClientConnection, */ ServerName};
use crate::secure_wipe::SecureString;
use crate::tokio_rustls::client::TlsStream;
use crate::{connect::ConnectInfo, inject_io_failure, AuthStyle, Options, ServerInfo};

/// Maintains a list of servers and establishes connections.
///
/// Clients use this helper to hold a list of known servers discovered through
/// INFO messages, reconnect when the connection is lost, and do exponential
/// backoff after failed connect attempts.
pub(crate) struct Connector {
    /// A map of servers and number of connect attempts.
    attempts: HashMap<Server, usize>,

    /// Configured options for establishing connections.
    options: Arc<Options>,

    /// TLS config.
    tls_config: Arc<ClientConfig>,
}

impl Connector {
    /// Creates a new connector with the URLs and options.
    pub(crate) fn new(url: &str, options: Arc<Options>) -> io::Result<Connector> {
        // Include system root certificates.
        //
        // On Windows, some certificates cannot be loaded by rustls
        // for whatever reason, so we simply skip them.
        // See https://github.com/ctz/rustls-native-certs/issues/5
        let roots = match rustls_native_certs::load_native_certs() {
            Ok(store) => store.into_iter().map(|c| c.0).collect(),
            Err(_) => Vec::new(),
        };
        let mut root_certs = crate::rustls::RootCertStore::empty();
        let (_added, _ignored) = root_certs.add_parsable_certificates(&roots);

        // Include user-provided certificates.
        for path in &options.certificates {
            let f = std::fs::File::open(path)?;
            let mut f = std::io::BufReader::new(f);
            let certs = rustls_pemfile::certs(&mut f)?;
            let (_added, _ignored) = root_certs.add_parsable_certificates(&certs);

            /*
            root_certs
                .add(contents.into())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid certificate file")
                })?;
             */
        }

        let tls_config = options
            .tls_client_config
            .clone()
            .with_safe_defaults()
            .with_root_certificates(root_certs);
        let tls_config =
            if let (Some(cert), Some(key)) = (&options.client_cert, &options.client_key) {
                tls_config
                    .with_single_cert(auth_utils::load_certs(cert)?, auth_utils::load_key(key)?)
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid client certificate and key pair: {}", err),
                        )
                    })?
            } else {
                tls_config.with_no_client_auth()
            };

        let mut connector = Connector {
            attempts: HashMap::new(),
            options,
            tls_config: Arc::new(tls_config),
        };
        // Add all URLs in the comma-separated list.
        for url in url.split(',') {
            connector.add_url(url)?;
        }

        Ok(connector)
    }

    /// Adds an URL to the list of servers.
    pub(crate) fn add_url(&mut self, url: &str) -> io::Result<()> {
        let server = Server::new(url)?;
        self.attempts.insert(server, 0);
        Ok(())
    }

    pub(crate) fn get_options(&self) -> Arc<Options> {
        self.options.clone()
    }

    /// Get the list of servers with enough reconnection attempts left
    fn get_servers(&mut self) -> io::Result<Vec<Server>> {
        let mut servers: Vec<Server> = self.attempts.keys().cloned().collect();

        servers = servers
            .into_iter()
            .filter(|server| {
                let reconnects = self.attempts.get_mut(server).unwrap();
                match self.options.max_reconnects.as_ref() {
                    Some(max) => max > reconnects,
                    None => true,
                }
            })
            .collect();

        if servers.is_empty() {
            Err(Error::new(
                ErrorKind::NotFound,
                "no servers remaining to connect to",
            ))
        } else {
            Ok(servers)
        }
    }

    /// Creates a new connection to one of the known URLs.
    ///
    /// If `use_backoff` is `true`, this method will try connecting in a loop
    /// and will back off after failed connect attempts.
    pub(crate) async fn connect(
        &mut self,
        use_backoff: bool,
    ) -> io::Result<(ServerInfo, NatsStream)> {
        // The last seen error, which gets returned if all connect attempts
        // fail.
        let mut last_err = Error::new(ErrorKind::AddrNotAvailable, "no socket addresses");

        loop {
            // Shuffle the list of servers.
            let mut servers: Vec<Server> = self.get_servers()?;
            fastrand::shuffle(&mut servers);

            // Iterate over the server list in random order.
            for server in &servers {
                // Calculate sleep duration for exponential backoff and bump the
                // reconnect counter.
                let reconnects = self.attempts.get_mut(server).unwrap();
                let sleep_duration = self
                    .options
                    .reconnect_delay_callback
                    .call(*reconnects)
                    .await;
                *reconnects += 1;

                // Resolve the server URL to socket addresses.
                let host = server.host();
                let port = server.port();

                // Inject random I/O failures when testing.
                let fault_injection = inject_io_failure();

                let lookup_res = fault_injection.and_then(|_| (host, port).to_socket_addrs());

                let mut addrs = match lookup_res {
                    Ok(addrs) => addrs.collect::<Vec<_>>(),
                    Err(err) => {
                        last_err = err;
                        continue;
                    }
                };

                // Shuffle the resolved socket addresses.
                fastrand::shuffle(&mut addrs);

                for addr in addrs {
                    // Sleep for some time if this is not the first connection
                    // attempt for this server.
                    if let Some(sleep_duration) = sleep_duration {
                        tokio::time::sleep(sleep_duration).await;
                    }

                    // Try connecting to this address.
                    let res = self.connect_addr(addr, server).await;

                    // Check if connecting worked out.
                    let (server_info, stream) = match res {
                        Ok(val) => val,
                        Err(err) => {
                            last_err = err;
                            continue;
                        }
                    };

                    // Add URLs discovered through the INFO message.
                    for url in &server_info.connect_urls {
                        self.add_url(url)?;
                    }

                    *self.attempts.get_mut(server).unwrap() = 0;
                    return Ok((server_info, stream));
                }
            }

            if !use_backoff {
                // All connect attempts have failed.
                return Err(last_err);
            }
        }
    }

    /// Attempts to establish a connection to a single socket address.
    async fn connect_addr(
        &self,
        addr: SocketAddr,
        server: &Server,
    ) -> io::Result<(ServerInfo, NatsStream)> {
        // Inject random I/O failures when testing.
        inject_io_failure()?;

        // Connect to the remote socket.
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        // Expect an INFO message.
        let mut line = crate::SecureVec::with_capacity(1024);
        while !line.ends_with(b"\r\n") {
            let byte = &mut [0];
            stream.read_exact(byte).await?;
            line.push(byte[0]);
        }
        let server_info = match proto::decode(&line[..]).await? {
            Some(ServerOp::Info(server_info)) => server_info,
            Some(op) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("expected INFO, received: {:?}", op),
                ));
            }
            None => {
                return Err(Error::new(ErrorKind::UnexpectedEof, "connection closed"));
            }
        };

        // Check if TLS authentication is required:
        // - Has `self.options.tls_required(true)` been set?
        // - Was the server address prefixed with `tls://`?
        // - Does the INFO line contain `tls_required: true`?
        let tls_required =
            self.options.tls_required || server.tls_required() || server_info.tls_required;

        // Upgrade to TLS if required.
        let mut stream = if tls_required {
            // Inject random I/O failures when testing.
            inject_io_failure()?;

            // Connect using TLS.
            let dns_name = ServerName::try_from(server_info.host.as_str())
                .or_else(|_| ServerName::try_from(server.host()))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot determine hostname for TLS connection",
                    )
                })?;
            NatsStream::new_tls(
                tokio_rustls::TlsConnector::from(self.tls_config.clone())
                    .connect(dns_name, stream)
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::NotConnected, e))?,
            )
        } else {
            NatsStream::new_tcp(stream)
        };

        // Data that will be formatted as a CONNECT message.
        let mut connect_info = ConnectInfo {
            tls_required,
            name: self.options.name.clone().map(SecureString::from),
            pedantic: false,
            verbose: false,
            lang: crate::LANG.to_string(),
            version: crate::VERSION.to_string(),
            protocol: crate::connect::Protocol::Dynamic,
            user: None,
            pass: None,
            auth_token: None,
            user_jwt: None,
            nkey: None,
            signature: None,
            echo: !self.options.no_echo,
            headers: true,
            no_responders: true,
        };

        // Fill in the info that authenticates the client.
        match &self.options.auth {
            AuthStyle::NoAuth => {}
            AuthStyle::UserPass(user, pass) => {
                connect_info.user = Some(SecureString::from(user.to_string()));
                connect_info.pass = Some(SecureString::from(pass.to_string()));
            }
            AuthStyle::Token(token) => {
                connect_info.auth_token = Some(token.to_string().into());
            }
            AuthStyle::Credentials { jwt_cb, sig_cb } => {
                let jwt = jwt_cb()?;
                let sig = sig_cb(server_info.nonce.as_bytes())?;
                connect_info.user_jwt = Some(jwt);
                connect_info.signature = Some(sig);
            }
            AuthStyle::NKey { nkey_cb, sig_cb } => {
                let nkey = nkey_cb()?;
                let sig = sig_cb(server_info.nonce.as_bytes())?;
                connect_info.nkey = Some(nkey);
                connect_info.signature = Some(sig);
            }
        }

        // If our server url had embedded username, check that here.
        if server.has_user_pass() {
            connect_info.user = server.username();
            connect_info.pass = server.password();
        }

        // Send CONNECT and PING messages.
        proto::encode(&mut stream, ClientOp::Connect(&connect_info)).await?;
        proto::encode(&mut stream, ClientOp::Ping).await?;
        stream.flush().await?;

        let mut reader = BufReader::new(stream.clone());

        // Wait for a PONG.
        loop {
            match proto::decode(&mut reader).await? {
                // If we get PONG, the server is happy and we're done
                // connecting.
                Some(ServerOp::Pong) => break,

                // Respond to a PING with a PONG.
                Some(ServerOp::Ping) => {
                    proto::encode(&mut stream, ClientOp::Pong).await?;
                    stream.flush().await?;
                }

                // No other operations should arrive at this time.
                Some(op) => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected line while connecting: {:?}", op),
                    ));
                }

                // Error if the connection was closed.
                None => {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "connection closed while waiting for the first PONG",
                    ));
                }
            }
        }

        Ok((server_info, stream))
    }
}

/// A parsed URL with defaults for port and scheme if needed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Server {
    url: Url,
}

impl Server {
    /// Returns if tls is required by the client for this server.
    fn tls_required(&self) -> bool {
        self.url.scheme() == "tls"
    }

    /// Returns if the server url had embedded username and password.
    fn has_user_pass(&self) -> bool {
        self.url.username() != ""
    }

    /// Returns the host.
    fn host(&self) -> &str {
        self.url.host_str().unwrap()
    }

    /// Returns the port.
    fn port(&self) -> u16 {
        self.url.port().unwrap()
    }

    /// Returns the optional username in the url.
    fn username(&self) -> Option<SecureString> {
        let user = self.url.username();
        if user.is_empty() {
            None
        } else {
            Some(SecureString::from(user.to_string()))
        }
    }

    /// Returns the optional password in the url.
    fn password(&self) -> Option<SecureString> {
        self.url
            .password()
            .map(|password| SecureString::from(password.to_string()))
    }

    /// Creates a new server from an URL.
    ///
    /// Returns an error if the URL cannot be parsed.
    fn new(raw_url: &str) -> io::Result<Server> {
        let mut url_str = raw_url.to_string();

        // Make sure this isn't a comma-separated URL list.
        if url_str.contains(',') {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "only one server URL should be passed to Server::new",
            ));
        }

        // Check for scheme. Url::parse requires it.
        if !url_str.contains("://") {
            url_str = format!("nats://{}", url_str);
        }

        let mut url = if let Ok(url) = Url::parse(&url_str) {
            url
        } else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("invalid URL provided: {}", url_str),
            ));
        };

        // Set default port.
        if url.port().is_none() {
            url.set_port(Some(4222)).ok();
        }

        Ok(Server { url })
    }
}

/// A raw NATS stream of bytes.
///
/// The stream uses the TCP protocol, optionally secured by TLS.
#[derive(Debug, Clone)]
pub(crate) struct NatsStream {
    flavor: Arc<Flavor>,
    //write_timeout: Option<Duration>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Flavor {
    Tcp(Mutex<TcpStream>),
    Tls(Mutex<TlsStream<TcpStream>>), //Tls(Box<Mutex<TlsStream<TcpStream>>>),
}

//struct TlsStream {
//    tcp: TcpStream,
//    session: ClientConnection,
//}

impl NatsStream {
    fn new_tcp(tcp: TcpStream) -> Self {
        tcp.set_nodelay(true).ok(); // ignore err if not supported
        Self {
            flavor: Arc::new(Flavor::Tcp(Mutex::new(tcp))),
        }
    }
    fn new_tls(tls: TlsStream<TcpStream>) -> Self {
        Self {
            flavor: Arc::new(Flavor::Tls(Mutex::new(tls))),
        }
    }

    //pub(crate) async fn set_write_timeout(&mut self, timeout: Option<Duration>) {
    //    self.write_timeout = timeout;
    //}

    /// Will attempt to shutdown the underlying stream.
    pub(crate) async fn shutdown(&mut self) {
        match Arc::<Flavor>::get_mut(&mut self.flavor).unwrap() {
            Flavor::Tcp(tcp) => {
                let tcp = tcp.get_mut();
                let _ = tcp.shutdown().await;
            }
            Flavor::Tls(tls) => {
                let tls = tls.get_mut();
                let _ = tls.get_mut().0.shutdown().await;
            }
        }
    }
}

macro_rules! impl_poll_flavors {
    ( $fname:ident, $for:ident ) => {
        fn $fname(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let flavor: &Flavor = self.flavor.borrow();
            match flavor {
                Flavor::Tcp(tcp) => {
                    if let Ok(mut guard) = tcp.try_lock() {
                        Pin::new(guard.deref_mut()).$fname(cx)
                    } else {
                        Poll::Pending
                    }
                }
                Flavor::Tls(tls) => {
                    if let Ok(mut guard) = tls.try_lock() {
                        Pin::new(guard.deref_mut()).$fname(cx)
                    } else {
                        Poll::Pending
                    }
                }
            }
        }
    };
}

impl AsyncRead for NatsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let flavor: &Flavor = self.flavor.borrow();
        match flavor {
            Flavor::Tcp(tcp) => {
                if let Ok(mut guard) = tcp.try_lock() {
                    Pin::new(guard.deref_mut()).poll_read(cx, buf)
                } else {
                    Poll::Pending
                }
            }
            Flavor::Tls(tls) => {
                if let Ok(mut guard) = tls.try_lock() {
                    Pin::new(guard.deref_mut()).poll_read(cx, buf)
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

#[async_trait]
impl AsyncWrite for NatsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let flavor: &Flavor = self.flavor.borrow();
        match flavor {
            Flavor::Tcp(tcp) => {
                if let Ok(mut guard) = tcp.try_lock() {
                    Pin::new(guard.deref_mut()).poll_write(cx, buf)
                } else {
                    Poll::Pending
                }
            }
            Flavor::Tls(tls) => {
                if let Ok(mut guard) = tls.try_lock() {
                    Pin::new(guard.deref_mut()).poll_write(cx, buf)
                } else {
                    Poll::Pending
                }
            }
        }
    }

    impl_poll_flavors!(poll_flush, NatsStream);
    impl_poll_flavors!(poll_shutdown, NatsStream);
}

//impl AsyncWriteExt for NatsStream {}
