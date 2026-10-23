use std::time::{Duration, Instant};

use super::bucket::{KBucket, UpdateOutcome};
use crate::contact::Contact;
use crate::id::{ID_BITS, NodeId};

/// One k-bucket per bit of the keyspace, indexed by distance from us.
///
/// The paper describes a binary tree of buckets that split as they fill up.
/// A flat array of 160 buckets ends up in the same place: the far buckets fill
/// and the near ones stay mostly empty, because there are exponentially fewer
/// ids close to ours. The array is a lot simpler to work with.
#[derive(Debug, Clone)]
pub struct RoutingTable {
    local_id: NodeId,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(local_id: NodeId, k: usize) -> Self {
        Self {
            local_id,
            buckets: (0..ID_BITS).map(|_| KBucket::new(k)).collect(),
        }
    }

    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// Records that we just heard from `contact`. See [`KBucket::update`].
    pub fn update(&mut self, contact: Contact) -> UpdateOutcome {
        match self.bucket_index(&contact.id) {
            Some(index) => self.buckets[index].update(contact),
            None => UpdateOutcome::Ignored,
        }
    }

    pub fn remove(&mut self, id: &NodeId) -> Option<Contact> {
        let index = self.bucket_index(id)?;
        self.buckets[index].remove(id)
    }

    /// See [`KBucket::mark_unresponsive`].
    pub fn mark_unresponsive(&mut self, id: &NodeId) -> bool {
        match self.bucket_index(id) {
            Some(index) => self.buckets[index].mark_unresponsive(id),
            None => false,
        }
    }

    pub fn contains(&self, id: &NodeId) -> bool {
        self.bucket_index(id)
            .is_some_and(|index| self.buckets[index].contains(id))
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(KBucket::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(KBucket::is_empty)
    }

    pub fn contacts(&self) -> impl Iterator<Item = &Contact> {
        self.buckets.iter().flat_map(KBucket::contacts)
    }

    pub fn buckets(&self) -> &[KBucket] {
        &self.buckets
    }

    /// The `count` contacts closest to `target`, nearest first.
    pub fn closest(&self, target: &NodeId, count: usize) -> Vec<Contact> {
        // Just scan everything. The table holds at most 160 * k entries and
        // usually far fewer, which is nothing next to a network round trip.
        let mut contacts: Vec<Contact> = self.contacts().copied().collect();
        contacts.sort_by_key(|c| c.id.distance(target));
        contacts.truncate(count);
        contacts
    }

    /// Notes that we just ran a lookup for `target`, so the bucket covering
    /// it doesn't need refreshing for a while.
    pub fn mark_looked_up(&mut self, target: &NodeId) {
        if let Some(index) = self.bucket_index(target) {
            self.buckets[index].mark_looked_up(Instant::now());
        }
    }

    /// Buckets that haven't seen a lookup in `max_age`.
    ///
    /// Only buckets from our nearest neighbour outwards are considered. The
    /// ones closer than that are empty because there's nobody there, and
    /// refreshing them would just be wasted lookups.
    pub fn stale_buckets(&self, max_age: Duration) -> Vec<usize> {
        let Some(nearest) = self.buckets.iter().position(|b| !b.is_empty()) else {
            return Vec::new();
        };
        let now = Instant::now();
        (nearest..ID_BITS)
            .filter(|&i| now.duration_since(self.buckets[i].last_lookup()) >= max_age)
            .collect()
    }

    fn bucket_index(&self, id: &NodeId) -> Option<usize> {
        self.local_id.distance(id).bucket_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    fn random_contact() -> Contact {
        let port = rand::random::<u16>().max(1024);
        Contact::new(
            NodeId::random(),
            SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        )
    }

    #[test]
    fn never_stores_itself() {
        let id = NodeId::random();
        let mut table = RoutingTable::new(id, 20);
        let me = Contact::new(id, "127.0.0.1:4000".parse().unwrap());
        assert_eq!(table.update(me), UpdateOutcome::Ignored);
        assert!(table.is_empty());
    }

    #[test]
    fn contacts_land_in_the_bucket_for_their_distance() {
        let local = NodeId::random();
        let mut table = RoutingTable::new(local, 20);
        for index in [0, 7, 80, 159] {
            let contact = Contact::new(
                local.random_in_bucket(index),
                "127.0.0.1:4000".parse().unwrap(),
            );
            table.update(contact);
            assert!(table.buckets()[index].contains(&contact.id));
        }
        assert_eq!(table.len(), 4);
    }

    #[test]
    fn closest_matches_a_brute_force_search() {
        // Large k so nothing gets turned away and we can compare against
        // everything we inserted.
        let mut table = RoutingTable::new(NodeId::random(), 1000);
        let contacts: Vec<Contact> = (0..300).map(|_| random_contact()).collect();
        for &c in &contacts {
            table.update(c);
        }

        for _ in 0..20 {
            let target = NodeId::random();
            let mut expected = contacts.clone();
            expected.sort_by_key(|c| c.id.distance(&target));
            expected.truncate(20);
            assert_eq!(table.closest(&target, 20), expected);
        }
    }

    #[test]
    fn closest_returns_what_it_has_when_table_is_small() {
        let mut table = RoutingTable::new(NodeId::random(), 20);
        for _ in 0..5 {
            table.update(random_contact());
        }
        assert_eq!(table.closest(&NodeId::random(), 20).len(), 5);
    }

    #[test]
    fn removal_by_id() {
        let mut table = RoutingTable::new(NodeId::random(), 20);
        let contact = random_contact();
        table.update(contact);
        assert!(table.contains(&contact.id));
        assert_eq!(table.remove(&contact.id), Some(contact));
        assert!(!table.contains(&contact.id));
    }

    #[test]
    fn stale_buckets_start_at_nearest_neighbour() {
        let local = NodeId::random();
        let mut table = RoutingTable::new(local, 20);
        assert!(table.stale_buckets(Duration::ZERO).is_empty());

        table.update(Contact::new(
            local.random_in_bucket(150),
            "127.0.0.1:4000".parse().unwrap(),
        ));
        assert_eq!(
            table.stale_buckets(Duration::ZERO),
            (150..ID_BITS).collect::<Vec<_>>()
        );
        // Buckets were all created just now, so none are an hour old.
        assert!(table.stale_buckets(Duration::from_secs(3600)).is_empty());
    }
}
