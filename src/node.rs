use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::ToSocketAddrs;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::contact::Contact;
use crate::error::{Error, Result};
use crate::id::NodeId;
use crate::protocol::{Body, MAX_DATAGRAM_LEN, Message, Request, Response};
use crate::routing::RoutingTable;
use crate::transport::{Reply, Transport};

/// A running Kademlia node.
///
/// The node answers requests from its peers in a background task for as long
/// as this handle is alive. Dropping it (or calling [`shutdown`](Self::shutdown))
/// stops the node.
pub struct Node {
    inner: Arc<Inner>,
    tasks: Vec<JoinHandle<()>>,
}

/// State shared between the public handle and the background tasks.
pub(crate) struct Inner {
    pub id: NodeId,
    pub config: Config,
    pub transport: Transport,
    pub table: Mutex<RoutingTable>,
}

impl Node {
    /// Binds a UDP socket to `addr` and starts a node with a random id.
    pub async fn bind(addr: impl ToSocketAddrs, config: Config) -> Result<Self> {
        Self::bind_with_id(NodeId::random(), addr, config).await
    }

    /// Like [`bind`](Self::bind), but with a chosen id.
    pub async fn bind_with_id(
        id: NodeId,
        addr: impl ToSocketAddrs,
        config: Config,
    ) -> Result<Self> {
        let transport = Transport::bind(addr, id, config.request_timeout).await?;
        let inner = Arc::new(Inner {
            id,
            table: Mutex::new(RoutingTable::new(id, config.k)),
            transport,
            config,
        });

        let receiver = tokio::spawn(receive_loop(Arc::clone(&inner)));
        Ok(Self {
            inner,
            tasks: vec![receiver],
        })
    }

    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.transport.local_addr()
    }

    /// A snapshot of every contact in the routing table.
    pub fn peers(&self) -> Vec<Contact> {
        self.inner
            .table
            .lock()
            .unwrap()
            .contacts()
            .copied()
            .collect()
    }

    /// Checks whether a node is listening at `addr`, and returns its id.
    pub async fn ping(&self, addr: SocketAddr) -> Result<NodeId> {
        let reply = self.inner.transport.request(addr, Request::Ping).await?;
        match reply.response {
            Response::Pong => Ok(reply.sender),
            _ => Err(Error::UnexpectedResponse),
        }
    }

    /// Joins the network through a node whose address we already know.
    pub async fn bootstrap(&self, seed: SocketAddr) -> Result<()> {
        // The receive loop adds the seed to our table when its pong arrives.
        self.ping(seed).await?;
        Ok(())
    }

    /// Stops the node. Same as dropping it, but easier to spot in code.
    pub fn shutdown(self) {}
}

impl Drop for Node {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.inner.id)
            .field("addr", &self.local_addr())
            .finish_non_exhaustive()
    }
}

impl Inner {
    async fn handle_message(self: &Arc<Self>, msg: Message, from: SocketAddr) {
        // Any message at all tells us the sender is alive and where it lives.
        self.observe(Contact::new(msg.sender, from));

        match msg.body {
            Body::Request(request) => {
                let response = self.handle_request(&msg.sender, request);
                let reply = Message::response(msg.txid, self.id, response);
                if let Err(e) = self.transport.send(from, &reply).await {
                    tracing::debug!(%from, error = %e, "failed to send response");
                }
            }
            Body::Response(response) => {
                let reply = Reply {
                    sender: msg.sender,
                    response,
                };
                self.transport.resolve(msg.txid, from, reply);
            }
        }
    }

    fn handle_request(&self, requester: &NodeId, request: Request) -> Response {
        match request {
            Request::Ping => Response::Pong,
            Request::FindNode { target } | Request::FindValue { key: target } => {
                Response::Nodes(self.closest_for(requester, &target))
            }
            Request::Store { .. } => Response::Rejected,
        }
    }

    fn observe(&self, contact: Contact) {
        self.table.lock().unwrap().update(contact);
    }

    /// Our `k` closest contacts to `target`, minus whoever asked. They
    /// already know about themselves.
    fn closest_for(&self, requester: &NodeId, target: &NodeId) -> Vec<Contact> {
        let k = self.config.k;
        let mut nodes = self.table.lock().unwrap().closest(target, k + 1);
        nodes.retain(|c| &c.id != requester);
        nodes.truncate(k);
        nodes
    }
}

async fn receive_loop(inner: Arc<Inner>) {
    let mut buf = vec![0u8; MAX_DATAGRAM_LEN];
    loop {
        let (len, from) = match inner.transport.recv(&mut buf).await {
            Ok(received) => received,
            // On Windows, an ICMP "port unreachable" from an earlier send
            // shows up as an error on the next receive. That happens every
            // time we talk to a node that has gone away, and it says nothing
            // about our own socket.
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => continue,
            Err(e) => {
                tracing::error!(error = %e, "socket receive failed, node stopping");
                return;
            }
        };

        match Message::decode(&buf[..len]) {
            Ok(msg) => inner.handle_message(msg, from).await,
            Err(e) => tracing::debug!(%from, error = %e, "dropping malformed datagram"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> Config {
        Config {
            request_timeout: Duration::from_millis(200),
            ..Config::default()
        }
    }

    async fn local_node() -> Node {
        Node::bind("127.0.0.1:0", test_config()).await.unwrap()
    }

    #[tokio::test]
    async fn ping_returns_peer_id() {
        let a = local_node().await;
        let b = local_node().await;
        assert_eq!(a.ping(b.local_addr()).await.unwrap(), b.id());
    }

    #[tokio::test]
    async fn both_sides_learn_about_each_other() {
        let a = local_node().await;
        let b = local_node().await;
        a.bootstrap(b.local_addr()).await.unwrap();

        assert_eq!(a.peers(), vec![Contact::new(b.id(), b.local_addr())]);
        assert_eq!(b.peers(), vec![Contact::new(a.id(), a.local_addr())]);
    }

    #[tokio::test]
    async fn ping_to_a_dead_node_times_out() {
        let a = local_node().await;
        let b = local_node().await;
        let addr = b.local_addr();
        b.shutdown();

        assert!(matches!(a.ping(addr).await, Err(Error::Timeout(_))));
    }

    #[tokio::test]
    async fn junk_datagrams_are_ignored() {
        let a = local_node().await;
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket
            .send_to(b"definitely not a message", a.local_addr())
            .await
            .unwrap();

        // Still alive and answering.
        let b = local_node().await;
        assert_eq!(b.ping(a.local_addr()).await.unwrap(), a.id());
    }
}
