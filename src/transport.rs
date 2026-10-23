//! Sending requests over UDP and pairing them up with their responses.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::id::NodeId;
use crate::protocol::{Message, Request, Response};

/// A response to one of our requests, along with who sent it.
#[derive(Debug)]
pub(crate) struct Reply {
    pub sender: NodeId,
    pub response: Response,
}

struct Pending {
    addr: SocketAddr,
    tx: oneshot::Sender<Reply>,
}

pub(crate) struct Transport {
    socket: UdpSocket,
    local_id: NodeId,
    local_addr: SocketAddr,
    timeout: Duration,
    pending: Mutex<HashMap<u64, Pending>>,
}

impl Transport {
    pub async fn bind(
        addr: impl ToSocketAddrs,
        local_id: NodeId,
        timeout: Duration,
    ) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr()?;
        Ok(Self {
            socket,
            local_id,
            local_addr,
            timeout,
            pending: Mutex::new(HashMap::new()),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Sends `request` to `addr` and waits for the matching response.
    ///
    /// Something else has to be calling [`recv`](Self::recv) and handing
    /// responses to [`resolve`](Self::resolve), or this will always time out.
    pub async fn request(&self, addr: SocketAddr, request: Request) -> Result<Reply> {
        let (tx, rx) = oneshot::channel();
        let txid = self.register(addr, tx);
        // Clears the pending entry however we leave this function, including
        // the caller dropping the future halfway through.
        let _guard = PendingGuard {
            pending: &self.pending,
            txid,
        };

        self.send(addr, &Message::request(txid, self.local_id, request))
            .await?;

        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            _ => Err(Error::Timeout(addr)),
        }
    }

    pub async fn send(&self, addr: SocketAddr, msg: &Message) -> Result<()> {
        let bytes = msg.encode()?;
        self.socket.send_to(&bytes, addr).await?;
        Ok(())
    }

    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(buf).await
    }

    /// Passes a response to the request waiting for it.
    ///
    /// Responses nobody is waiting for (late, duplicated, or forged) are
    /// dropped. So are responses from an address other than the one the
    /// request went to: the random txid makes forging a reply hard, and this
    /// makes it pointless from anywhere else.
    pub fn resolve(&self, txid: u64, from: SocketAddr, reply: Reply) {
        let mut pending = self.pending.lock().unwrap();
        if let Entry::Occupied(entry) = pending.entry(txid)
            && entry.get().addr == from
        {
            // The receiver may have just timed out; that's fine.
            let _ = entry.remove().tx.send(reply);
        }
    }

    fn register(&self, addr: SocketAddr, tx: oneshot::Sender<Reply>) -> u64 {
        let mut pending = self.pending.lock().unwrap();
        loop {
            let txid = rand::random();
            if let Entry::Vacant(slot) = pending.entry(txid) {
                slot.insert(Pending { addr, tx });
                return txid;
            }
        }
    }
}

struct PendingGuard<'a> {
    pending: &'a Mutex<HashMap<u64, Pending>>,
    txid: u64,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.txid);
    }
}
