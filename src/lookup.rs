//! Iterative lookups, the heart of Kademlia.
//!
//! A lookup starts from the closest contacts in our own table and keeps asking
//! the closest nodes it has heard of for nodes even closer to the target, with
//! up to `alpha` requests in flight at once. It finishes when the `k` closest
//! nodes it knows about have all answered, or, for a value lookup, as soon as
//! someone hands over the value.
//!
//! The paper phrases termination in terms of rounds: once a round turns up
//! nobody closer, query the remaining unqueried nodes among the `k` closest.
//! Doing it continuously ends in the same place without waiting for the
//! slowest node in each round.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::task::JoinSet;

use crate::contact::Contact;
use crate::id::{Distance, NodeId};
use crate::node::Inner;
use crate::protocol::{Request, Response};

/// A value lookup that found something.
pub(crate) struct FoundValue {
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Node,
    Value,
}

enum Outcome {
    Nodes(Vec<Contact>),
    Value(FoundValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    NotAsked,
    InFlight,
    Answered,
    Failed,
}

struct Candidate {
    contact: Contact,
    state: State,
}

/// Finds the `k` nodes closest to `target` that are reachable right now,
/// nearest first.
pub(crate) async fn find_node(inner: &Arc<Inner>, target: NodeId) -> Vec<Contact> {
    match run(inner, target, Kind::Node).await {
        Outcome::Nodes(nodes) => nodes,
        Outcome::Value(_) => unreachable!("node lookups never accept a value"),
    }
}

/// Looks for the value stored under `key`.
pub(crate) async fn find_value(inner: &Arc<Inner>, key: NodeId) -> Option<FoundValue> {
    match run(inner, key, Kind::Value).await {
        Outcome::Value(found) => Some(found),
        Outcome::Nodes(_) => None,
    }
}

async fn run(inner: &Arc<Inner>, target: NodeId, kind: Kind) -> Outcome {
    let k = inner.config.k;

    // XOR is a bijection, so every id is at a different distance from the
    // target. That makes the distance a fine key, and keeps the map sorted
    // nearest first for free.
    let mut shortlist: BTreeMap<Distance, Candidate> = BTreeMap::new();
    let seeds = {
        let mut table = inner.table.lock().unwrap();
        table.mark_looked_up(&target);
        table.closest(&target, k)
    };
    for contact in seeds {
        shortlist.insert(
            contact.id.distance(&target),
            Candidate {
                contact,
                state: State::NotAsked,
            },
        );
    }

    let mut in_flight = JoinSet::new();
    loop {
        // Keep `alpha` requests going, always to the closest node we haven't
        // asked yet. Only the `k` closest live candidates are worth asking:
        // anything further out can't end up in the result.
        while in_flight.len() < inner.config.alpha {
            let Some(next) = shortlist
                .values_mut()
                .filter(|c| c.state != State::Failed)
                .take(k)
                .find(|c| c.state == State::NotAsked)
            else {
                break;
            };
            next.state = State::InFlight;

            let request = match kind {
                Kind::Node => Request::FindNode { target },
                Kind::Value => Request::FindValue { key: target },
            };
            let inner = Arc::clone(inner);
            let contact = next.contact;
            in_flight.spawn(async move {
                let result = inner.rpc(&contact, request).await;
                (contact, result)
            });
        }

        // Nothing in flight and nobody left to ask: we're done.
        let Some(joined) = in_flight.join_next().await else {
            break;
        };
        let (contact, result) = joined.expect("lookup request task panicked");

        let candidate = shortlist
            .get_mut(&contact.id.distance(&target))
            .expect("answer from a node that isn't on the shortlist");
        match result {
            Ok(Response::Nodes(nodes)) => {
                candidate.state = State::Answered;
                // An honest node never sends more than k. Don't let a
                // dishonest one flood the shortlist.
                for node in nodes.into_iter().take(k) {
                    if node.id == inner.id {
                        continue;
                    }
                    shortlist
                        .entry(node.id.distance(&target))
                        .or_insert(Candidate {
                            contact: node,
                            state: State::NotAsked,
                        });
                }
            }
            Ok(Response::Value { value, .. }) if kind == Kind::Value => {
                // Returning drops `in_flight`, which cancels whatever is
                // still outstanding.
                return Outcome::Value(FoundValue { value });
            }
            Ok(_) | Err(_) => candidate.state = State::Failed,
        }
    }

    let nodes = shortlist
        .into_values()
        .filter(|c| c.state == State::Answered)
        .take(k)
        .map(|c| c.contact)
        .collect();
    Outcome::Nodes(nodes)
}
