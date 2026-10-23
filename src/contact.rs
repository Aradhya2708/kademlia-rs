use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;

/// A peer we know about: its id and the address we last heard from it on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Contact {
    pub id: NodeId,
    pub addr: SocketAddr,
}

impl Contact {
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Self { id, addr }
    }
}
