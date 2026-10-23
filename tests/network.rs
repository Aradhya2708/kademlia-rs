//! End-to-end tests that run whole networks of nodes on localhost.

use std::time::Duration;

use kademlia::{Config, Node, NodeId};

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
async fn lone_node_still_stores_values() {
    let node = Node::bind("127.0.0.1:0", config()).await.unwrap();
    assert_eq!(node.put(b"k", b"v".to_vec()).await.unwrap(), 1);
    assert_eq!(node.get(b"k").await.as_deref(), Some(&b"v"[..]));
}
