//! A Kademlia distributed hash table over UDP, built on tokio.
//!
//! This follows the design in Maymounkov and Mazières' paper, [*Kademlia: A
//! Peer-to-peer Information System Based on the XOR Metric*][paper]: 160-bit
//! ids, XOR distance, k-buckets with a replacement cache, iterative parallel
//! lookups, caching along the lookup path, and periodic bucket refreshes and
//! republishing.
//!
//! [paper]: https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf
//!
//! # Example
//!
//! ```
//! use kademlia::{Config, Node};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> kademlia::Result<()> {
//! let alice = Node::bind("127.0.0.1:0", Config::default()).await?;
//! let bob = Node::bind("127.0.0.1:0", Config::default()).await?;
//!
//! // Bob joins the network through Alice.
//! bob.bootstrap(alice.local_addr()).await?;
//!
//! alice.put(b"greeting", b"hello".to_vec()).await?;
//! assert_eq!(bob.get(b"greeting").await, Some(b"hello".to_vec()));
//! # Ok(())
//! # }
//! ```
//!
//! [`Node`] is the main entry point. The [`id`], [`routing`] and [`protocol`]
//! modules are public too, in case you want to build something different on
//! top of the same pieces.

mod config;
mod contact;
mod error;
pub mod id;
mod lookup;
mod node;
pub mod protocol;
pub mod routing;
mod store;
mod transport;

pub use config::Config;
pub use contact::Contact;
pub use error::{Error, Result};
pub use id::{Distance, NodeId};
pub use node::Node;
