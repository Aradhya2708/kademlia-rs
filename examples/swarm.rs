//! Starts a swarm of nodes on localhost, stores a few values through some of
//! them and reads the values back through others.
//!
//! ```text
//! cargo run --example swarm
//! cargo run --example swarm -- 200
//! RUST_LOG=kademlia=debug cargo run --example swarm
//! ```

use std::time::Instant;

use kademlia::{Config, Node};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> kademlia::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let size: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(50);

    let started = Instant::now();
    let seed = Node::bind("127.0.0.1:0", Config::default()).await?;
    let seed_addr = seed.local_addr();
    let mut nodes = vec![seed];
    for _ in 1..size {
        let node = Node::bind("127.0.0.1:0", Config::default()).await?;
        node.bootstrap(seed_addr).await?;
        nodes.push(node);
    }

    let average_peers =
        nodes.iter().map(|n| n.peers().len()).sum::<usize>() as f64 / nodes.len() as f64;
    println!(
        "started {size} nodes in {:.2?}, each knows {average_peers:.1} peers on average",
        started.elapsed()
    );

    for i in 0..10 {
        let key = format!("key-{i}");
        let writer = &nodes[(i * 7) % size];
        let reader = &nodes[(i * 13 + 5) % size];

        let copies = writer
            .put(key.as_bytes(), format!("value {i}").into_bytes())
            .await?;

        let lookup = Instant::now();
        let value = reader.get(key.as_bytes()).await;
        let value = value.map(|v| String::from_utf8_lossy(&v).into_owned());
        println!(
            "{key}: stored on {copies} nodes, read back {value:?} in {:.2?}",
            lookup.elapsed()
        );
    }

    Ok(())
}
