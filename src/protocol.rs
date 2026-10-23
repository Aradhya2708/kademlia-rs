//! The messages nodes exchange.
//!
//! Each message is one UDP datagram, encoded with postcard. Requests carry a
//! random transaction id that the response echoes back, which is how the
//! requester matches the two up. Every message also carries the sender's id,
//! so the receiver learns about peers as a side effect of normal traffic.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::contact::Contact;
use crate::error::Result;
use crate::id::NodeId;

/// The largest payload a single UDP datagram can carry over IPv4.
pub const MAX_DATAGRAM_LEN: usize = 65_507;

/// The largest value a node will store. This leaves room under
/// [`MAX_DATAGRAM_LEN`] for the rest of a STORE message.
pub const MAX_VALUE_LEN: usize = 60 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub txid: u64,
    pub sender: NodeId,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Body {
    Request(Request),
    Response(Response),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    Ping,
    /// Asks the receiver to hold `value` under `key` for up to `ttl`.
    Store {
        key: NodeId,
        value: Vec<u8>,
        ttl: Duration,
    },
    FindNode {
        target: NodeId,
    },
    FindValue {
        key: NodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Pong,
    Stored,
    /// The request made sense but the receiver won't act on it, for example
    /// a STORE that arrives when the record store is full.
    Rejected,
    /// The receiver's closest known contacts to the requested id. This is
    /// also the answer to FIND_VALUE when the receiver doesn't have the value.
    Nodes(Vec<Contact>),
    /// A FIND_VALUE hit, with how much longer the record has to live.
    Value {
        value: Vec<u8>,
        ttl: Duration,
    },
}

impl Message {
    pub fn request(txid: u64, sender: NodeId, request: Request) -> Self {
        Self {
            txid,
            sender,
            body: Body::Request(request),
        }
    }

    pub fn response(txid: u64, sender: NodeId, response: Response) -> Self {
        Self {
            txid,
            sender,
            body: Body::Response(response),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn round_trip(body: Body) {
        let msg = Message {
            txid: rand::random(),
            sender: NodeId::random(),
            body,
        };
        let bytes = msg.encode().unwrap();
        assert_eq!(Message::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn requests_round_trip() {
        round_trip(Body::Request(Request::Ping));
        round_trip(Body::Request(Request::Store {
            key: NodeId::random(),
            value: b"some value".to_vec(),
            ttl: Duration::from_secs(3600),
        }));
        round_trip(Body::Request(Request::FindNode {
            target: NodeId::random(),
        }));
        round_trip(Body::Request(Request::FindValue {
            key: NodeId::random(),
        }));
    }

    #[test]
    fn responses_round_trip() {
        let v4: SocketAddr = "192.0.2.1:4000".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::1]:4000".parse().unwrap();
        round_trip(Body::Response(Response::Pong));
        round_trip(Body::Response(Response::Stored));
        round_trip(Body::Response(Response::Rejected));
        round_trip(Body::Response(Response::Nodes(vec![
            Contact::new(NodeId::random(), v4),
            Contact::new(NodeId::random(), v6),
        ])));
        round_trip(Body::Response(Response::Value {
            value: vec![0xab; 100],
            ttl: Duration::from_millis(1500),
        }));
    }

    #[test]
    fn largest_store_fits_in_a_datagram() {
        let msg = Message::request(
            u64::MAX,
            NodeId::random(),
            Request::Store {
                key: NodeId::random(),
                value: vec![0xff; MAX_VALUE_LEN],
                ttl: Duration::MAX,
            },
        );
        assert!(msg.encode().unwrap().len() <= MAX_DATAGRAM_LEN);
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(Message::decode(&[]).is_err());
        assert!(Message::decode(&[0xff; 7]).is_err());
    }
}
