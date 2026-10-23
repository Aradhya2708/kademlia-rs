//! End-to-end tests that run whole networks of nodes on localhost.

use std::sync::Arc;
use std::time::Duration;

use kademlia::{Config, Node, NodeId};
use tokio::task::JoinSet;

const K: usize = 8;

fn config() -> Config {
    Config {
        k: K,
        request_timeout: Duration::from_millis(300),
        ..Config::default()
    }
}

/// Starts `size` nodes and bootstraps each one off the first.
async fn network(size: usize, config: Config) -> Vec<Node> {
    let seed = Node::bind("127.0.0.1:0", config.clone()).await.unwrap();
    let seed_addr = seed.local_addr();
    let mut nodes = vec![seed];
    for _ in 1..size {
        let node = Node::bind("127.0.0.1:0", config.clone()).await.unwrap();
        node.bootstrap(seed_addr).await.unwrap();
        nodes.push(node);
    }
    nodes
}

/// The `count` ids closest to `target` out of every node except `exclude`.
fn true_closest(nodes: &[Node], target: &NodeId, exclude: &NodeId, count: usize) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = nodes
        .iter()
        .map(Node::id)
        .filter(|id| id != exclude)
        .collect();
    ids.sort_by_key(|id| id.distance(target));
    ids.truncate(count);
    ids
}

#[tokio::test]
async fn bootstrap_discovers_more_than_the_seed() {
    let nodes = network(20, config()).await;
    let newest = nodes.last().unwrap();
    assert!(
        newest.peers().len() > 1,
        "only knows {} peers",
        newest.peers().len()
    );
}

#[tokio::test]
async fn lookup_finds_the_true_closest_nodes() {
    let nodes = network(40, config()).await;
    for searcher in nodes.iter().step_by(4) {
        let target = NodeId::random();
        let found: Vec<NodeId> = searcher
            .find_node(target)
            .await
            .iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(found, true_closest(&nodes, &target, &searcher.id(), K));
    }
}

#[tokio::test]
async fn lookup_routes_around_dead_nodes() {
    let mut nodes = network(30, config()).await;
    let searcher = nodes.remove(0);
    let target = NodeId::random();

    // Take down the k nodes closest to the target. Every routing table in
    // the network still has them.
    nodes.sort_by_key(|n| n.id().distance(&target));
    let survivors = nodes.split_off(K);
    drop(nodes);

    // We can't expect the exact k closest survivors here: every peer's
    // answer is still full of the dead nodes, so some survivors may not be
    // mentioned by anyone until tables heal. But the lookup must not give up,
    // and everything it returns must be alive.
    let found = searcher.find_node(target).await;
    assert_eq!(found.len(), K);
    for contact in &found {
        assert!(survivors.iter().any(|n| n.id() == contact.id));
    }
}

#[tokio::test]
async fn value_put_on_one_node_can_be_read_from_any_other() {
    let nodes = network(30, config()).await;
    nodes[7].put(b"answer", b"42".to_vec()).await.unwrap();

    for node in &nodes {
        assert_eq!(node.get(b"answer").await.as_deref(), Some(&b"42"[..]));
    }
}

#[tokio::test]
async fn value_is_stored_on_k_nodes_plus_the_publisher() {
    let nodes = network(30, config()).await;
    let copies = nodes[0].put(b"key", b"value".to_vec()).await.unwrap();
    assert_eq!(copies, K + 1);

    let held: usize = nodes.iter().map(Node::record_count).sum();
    assert_eq!(held, copies);
}

#[tokio::test]
async fn missing_key_is_none() {
    let nodes = network(10, config()).await;
    assert_eq!(nodes[3].get(b"never stored").await, None);
}

#[tokio::test]
async fn oversized_values_are_refused() {
    let node = Node::bind("127.0.0.1:0", config()).await.unwrap();
    let huge = vec![0u8; kademlia::protocol::MAX_VALUE_LEN + 1];
    assert!(matches!(
        node.put(b"big", huge).await,
        Err(kademlia::Error::ValueTooLarge(_))
    ));
}

#[tokio::test]
async fn records_expire_everywhere() {
    let config = Config {
        record_ttl: Duration::from_millis(300),
        republish_interval: Duration::from_millis(200),
        ..config()
    };
    let nodes = network(5, config).await;
    nodes[1].put(b"short lived", vec![1, 2, 3]).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(nodes[2].get(b"short lived").await, None);
    assert_eq!(nodes.iter().map(Node::record_count).sum::<usize>(), 0);
}

#[tokio::test]
async fn republishing_keeps_values_alive_when_holders_leave() {
    let config = Config {
        republish_interval: Duration::from_millis(500),
        ..config()
    };
    let mut nodes = network(30, config).await;
    let publisher = nodes.remove(0);
    publisher
        .put(b"durable", b"still here".to_vec())
        .await
        .unwrap();

    // Take down every node that got a copy, so only the publisher has it.
    nodes.retain(|node| node.record_count() == 0);

    // Give the publisher time to push the value out to the nodes that are
    // now closest, then take it down too.
    tokio::time::sleep(Duration::from_secs(3)).await;
    publisher.shutdown();

    for node in nodes.iter().step_by(5) {
        assert_eq!(
            node.get(b"durable").await.as_deref(),
            Some(&b"still here"[..])
        );
    }
}

#[tokio::test]
async fn lone_node_still_stores_values() {
    let node = Node::bind("127.0.0.1:0", config()).await.unwrap();
    assert_eq!(node.put(b"k", b"v".to_vec()).await.unwrap(), 1);
    assert_eq!(node.get(b"k").await.as_deref(), Some(&b"v"[..]));
}

#[tokio::test]
async fn late_joiners_can_read_older_values() {
    let mut nodes = network(15, config()).await;
    nodes[4]
        .put(b"old news", b"still relevant".to_vec())
        .await
        .unwrap();

    // Join through a node other than the original seed, for variety.
    let entry = nodes[9].local_addr();
    for _ in 0..15 {
        let node = Node::bind("127.0.0.1:0", config()).await.unwrap();
        node.bootstrap(entry).await.unwrap();
        nodes.push(node);
    }

    for node in &nodes[15..] {
        assert_eq!(
            node.get(b"old news").await.as_deref(),
            Some(&b"still relevant"[..])
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_do_not_step_on_each_other() {
    let nodes: Vec<_> = network(25, config())
        .await
        .into_iter()
        .map(Arc::new)
        .collect();

    let mut writers = JoinSet::new();
    for (i, node) in nodes.iter().enumerate() {
        let node = Arc::clone(node);
        writers.spawn(async move {
            let key = format!("key-{i}");
            node.put(key.as_bytes(), i.to_string().into_bytes())
                .await
                .unwrap();
        });
    }
    writers.join_all().await;

    for (i, node) in nodes.iter().rev().enumerate() {
        let key = format!("key-{i}");
        assert_eq!(
            node.get(key.as_bytes()).await,
            Some(i.to_string().into_bytes())
        );
    }
}
