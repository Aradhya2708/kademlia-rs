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
