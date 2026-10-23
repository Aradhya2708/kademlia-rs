use std::collections::HashSet;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::ToSocketAddrs;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::contact::Contact;
use crate::error::{Error, Result};
use crate::id::NodeId;
use crate::lookup;
use crate::protocol::{Body, MAX_DATAGRAM_LEN, MAX_VALUE_LEN, Message, Request, Response};
use crate::routing::{RoutingTable, UpdateOutcome};
use crate::store::RecordStore;
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
    pub store: Mutex<RecordStore>,
    /// Contacts we're currently pinging to decide whether to evict them.
    liveness_checks: Mutex<HashSet<NodeId>>,
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
            store: Mutex::new(RecordStore::new(config.max_records)),
            transport,
            config,
            liveness_checks: Mutex::new(HashSet::new()),
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

    /// How many records this node is holding for the network.
    pub fn record_count(&self) -> usize {
        self.inner.store.lock().unwrap().len()
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

        // Looking ourselves up fills in the buckets near our own id, and
        // tells the nodes there that we exist. Then refresh everything
        // further out so the rest of the table isn't empty.
        lookup::find_node(&self.inner, self.id()).await;
        self.inner.refresh_buckets(Duration::ZERO).await;
        Ok(())
    }

    /// Finds the `k` nodes closest to `target` in the network.
    pub async fn find_node(&self, target: NodeId) -> Vec<Contact> {
        lookup::find_node(&self.inner, target).await
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
        let now = Instant::now();
        match request {
            Request::Ping => Response::Pong,
            Request::FindNode { target } => Response::Nodes(self.closest_for(requester, &target)),
            Request::FindValue { key } => {
                let hit = self
                    .store
                    .lock()
                    .unwrap()
                    .get(&key, now)
                    .map(|r| Response::Value {
                        value: r.value.clone(),
                        ttl: r.ttl(now),
                    });
                hit.unwrap_or_else(|| Response::Nodes(self.closest_for(requester, &key)))
            }
            Request::Store { key, value, ttl } => {
                if value.len() > MAX_VALUE_LEN {
                    return Response::Rejected;
                }
                // Don't let peers make us hold things longer than we would
                // on our own.
                let ttl = ttl.min(self.config.record_ttl);
                if self.store.lock().unwrap().put(key, value, ttl, now) {
                    Response::Stored
                } else {
                    Response::Rejected
                }
            }
        }
    }

    fn observe(self: &Arc<Self>, contact: Contact) {
        let outcome = self.table.lock().unwrap().update(contact);
        if let UpdateOutcome::Full { least_recent } = outcome {
            self.check_liveness(least_recent);
        }
    }

    /// Pings a contact that's in line to be evicted from a full bucket.
    ///
    /// If it answers, the receive loop moves it to the back of its bucket and
    /// the newcomer stays in the replacement cache. If it doesn't, we drop it
    /// and the newest replacement takes its slot.
    fn check_liveness(self: &Arc<Self>, contact: Contact) {
        // A busy bucket can ask about the same contact many times over while
        // the first ping is still out. One is enough.
        if !self.liveness_checks.lock().unwrap().insert(contact.id) {
            return;
        }

        let inner = Arc::clone(self);
        tokio::spawn(async move {
            if inner.rpc(&contact, Request::Ping).await.is_err() {
                tracing::debug!(peer = ?contact.id, "evicting unresponsive contact");
                inner.table.lock().unwrap().remove(&contact.id);
            }
            inner.liveness_checks.lock().unwrap().remove(&contact.id);
        });
    }

    /// Sends a request to a contact we already know, and checks that the
    /// answer really came from it.
    ///
    /// Contacts that don't answer are reported to the routing table, which
    /// may evict them.
    pub(crate) async fn rpc(&self, contact: &Contact, request: Request) -> Result<Response> {
        match self.transport.request(contact.addr, request).await {
            Ok(reply) if reply.sender == contact.id => Ok(reply.response),
            Ok(reply) => {
                // A different node lives at that address now.
                tracing::debug!(
                    expected = ?contact.id,
                    actual = ?reply.sender,
                    "node at {} changed id",
                    contact.addr
                );
                self.table.lock().unwrap().remove(&contact.id);
                Err(Error::UnexpectedResponse)
            }
            Err(e) => {
                self.table.lock().unwrap().mark_unresponsive(&contact.id);
                Err(e)
            }
        }
    }

    /// Runs a lookup for a random id in every bucket that hasn't seen one in
    /// `max_age`.
    pub(crate) async fn refresh_buckets(self: &Arc<Self>, max_age: Duration) {
        let stale = self.table.lock().unwrap().stale_buckets(max_age);
        for index in stale {
            lookup::find_node(self, self.id.random_in_bucket(index)).await;
        }
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

    /// Three nodes where `b` and `c` share a bucket in `a`'s table, and that
    /// bucket only has room for one of them.
    async fn crowded_bucket() -> (Node, Node, Node) {
        let id = |first: u8, last: u8| {
            let mut bytes = [0u8; crate::id::ID_LEN];
            bytes[0] = first;
            bytes[crate::id::ID_LEN - 1] = last;
            NodeId::from_bytes(bytes)
        };
        let config = Config {
            k: 1,
            ..test_config()
        };
        let a = Node::bind_with_id(id(0, 0), "127.0.0.1:0", config.clone());
        let b = Node::bind_with_id(id(0x80, 1), "127.0.0.1:0", config.clone());
        let c = Node::bind_with_id(id(0x80, 2), "127.0.0.1:0", config);
        (a.await.unwrap(), b.await.unwrap(), c.await.unwrap())
    }

    #[tokio::test]
    async fn full_bucket_keeps_a_live_contact() {
        let (a, b, c) = crowded_bucket().await;
        b.ping(a.local_addr()).await.unwrap();
        c.ping(a.local_addr()).await.unwrap();

        // a pings b to check on it, b answers, and c stays out.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(a.peers(), vec![Contact::new(b.id(), b.local_addr())]);
    }

    #[tokio::test]
    async fn full_bucket_replaces_a_dead_contact() {
        let (a, b, c) = crowded_bucket().await;
        b.ping(a.local_addr()).await.unwrap();
        b.shutdown();
        c.ping(a.local_addr()).await.unwrap();

        // a's ping to b times out, so c gets promoted from the replacement cache.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(a.peers(), vec![Contact::new(c.id(), c.local_addr())]);
    }

    #[tokio::test]
    async fn stored_value_is_served_by_find_value() {
        let a = local_node().await;
        let b = local_node().await;
        let b_contact = Contact::new(b.id(), b.local_addr());
        let key = NodeId::hash(b"greeting");

        let store = Request::Store {
            key,
            value: b"hello".to_vec(),
            ttl: Duration::from_secs(60),
        };
        assert_eq!(
            a.inner.rpc(&b_contact, store).await.unwrap(),
            Response::Stored
        );
        assert_eq!(b.record_count(), 1);

        let found = a.inner.rpc(&b_contact, Request::FindValue { key }).await;
        match found.unwrap() {
            Response::Value { value, ttl } => {
                assert_eq!(value, b"hello");
                assert!(ttl <= Duration::from_secs(60));
            }
            other => panic!("expected a value, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn find_value_miss_returns_nodes() {
        let a = local_node().await;
        let b = local_node().await;
        let b_contact = Contact::new(b.id(), b.local_addr());

        let key = NodeId::hash(b"nobody has this");
        let found = a.inner.rpc(&b_contact, Request::FindValue { key }).await;
        assert!(matches!(found.unwrap(), Response::Nodes(_)));
    }

    #[tokio::test]
    async fn store_clamps_ttl_to_our_own_limit() {
        let config = Config {
            record_ttl: Duration::from_secs(10),
            ..test_config()
        };
        let a = local_node().await;
        let b = Node::bind("127.0.0.1:0", config).await.unwrap();
        let b_contact = Contact::new(b.id(), b.local_addr());
        let key = NodeId::hash(b"key");

        let store = Request::Store {
            key,
            value: vec![1],
            ttl: Duration::MAX,
        };
        a.inner.rpc(&b_contact, store).await.unwrap();

        let ttl = b
            .inner
            .store
            .lock()
            .unwrap()
            .get(&key, Instant::now())
            .unwrap()
            .ttl(Instant::now());
        assert!(ttl <= Duration::from_secs(10));
    }

    #[tokio::test]
    async fn store_rejects_when_full() {
        let config = Config {
            max_records: 1,
            ..test_config()
        };
        let a = local_node().await;
        let b = Node::bind("127.0.0.1:0", config).await.unwrap();
        let b_contact = Contact::new(b.id(), b.local_addr());

        let store = |key: &[u8]| Request::Store {
            key: NodeId::hash(key),
            value: vec![1],
            ttl: Duration::from_secs(60),
        };
        assert_eq!(
            a.inner.rpc(&b_contact, store(b"one")).await.unwrap(),
            Response::Stored
        );
        assert_eq!(
            a.inner.rpc(&b_contact, store(b"two")).await.unwrap(),
            Response::Rejected
        );
    }
}
