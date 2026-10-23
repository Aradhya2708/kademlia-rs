use std::collections::VecDeque;
use std::time::Instant;

use crate::contact::Contact;
use crate::id::NodeId;

/// What happened when a contact was offered to a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// The contact was new and there was room for it.
    Inserted,
    /// The contact was already known and has moved to the most-recent end.
    Refreshed,
    /// The bucket is full. The contact went into the replacement cache, and
    /// the caller should check whether `least_recent` is still alive.
    Full { least_recent: Contact },
    /// The contact was ourselves, which never goes in the table.
    Ignored,
}

/// Holds up to `k` contacts that fall in one distance range from us.
///
/// Contacts are ordered by when we last heard from them, least recent at the
/// front. The paper observes that a node which has been up for a long time is
/// likely to stay up, so a full bucket keeps its existing entries rather than
/// letting newcomers push them out. Newcomers wait in a replacement cache
/// instead and get promoted when an existing entry turns out to be dead.
#[derive(Debug, Clone)]
pub struct KBucket {
    entries: VecDeque<Contact>,
    replacements: VecDeque<Contact>,
    capacity: usize,
    last_lookup: Instant,
}

impl KBucket {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "bucket capacity must be at least 1");
        Self {
            entries: VecDeque::with_capacity(capacity),
            replacements: VecDeque::new(),
            capacity,
            last_lookup: Instant::now(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    /// Contacts in the bucket, least recently seen first.
    pub fn contacts(&self) -> impl Iterator<Item = &Contact> {
        self.entries.iter()
    }

    pub fn contains(&self, id: &NodeId) -> bool {
        self.position(id).is_some()
    }

    /// Records that we just heard from `contact`.
    pub fn update(&mut self, contact: Contact) -> UpdateOutcome {
        if let Some(pos) = self.position(&contact.id) {
            // Take the new address as well, in case the node came back on a
            // different port.
            self.entries.remove(pos);
            self.entries.push_back(contact);
            return UpdateOutcome::Refreshed;
        }

        if !self.is_full() {
            self.remove_replacement(&contact.id);
            self.entries.push_back(contact);
            return UpdateOutcome::Inserted;
        }

        self.remove_replacement(&contact.id);
        self.replacements.push_back(contact);
        if self.replacements.len() > self.capacity {
            self.replacements.pop_front();
        }
        UpdateOutcome::Full {
            least_recent: self.entries[0],
        }
    }

    /// Drops a contact from the bucket and fills its slot with the most
    /// recently seen replacement, if there is one.
    ///
    /// Returns the removed contact. A matching entry in the replacement cache
    /// is dropped as well, but isn't returned.
    pub fn remove(&mut self, id: &NodeId) -> Option<Contact> {
        let Some(pos) = self.position(id) else {
            self.remove_replacement(id);
            return None;
        };
        let removed = self.entries.remove(pos);
        if let Some(replacement) = self.replacements.pop_back() {
            self.entries.push_back(replacement);
        }
        removed
    }

    /// Called when a contact failed to answer a request.
    ///
    /// UDP drops packets now and then, so a single timeout isn't reason enough
    /// to forget a node. We only evict it if someone is already waiting for
    /// the slot. Returns whether the contact was evicted.
    pub fn mark_unresponsive(&mut self, id: &NodeId) -> bool {
        if self.replacements.is_empty() {
            return false;
        }
        self.remove(id).is_some()
    }

    /// When a lookup last targeted this bucket's range.
    pub fn last_lookup(&self) -> Instant {
        self.last_lookup
    }

    pub fn mark_looked_up(&mut self, at: Instant) {
        self.last_lookup = at;
    }

    fn position(&self, id: &NodeId) -> Option<usize> {
        self.entries.iter().position(|c| &c.id == id)
    }

    fn remove_replacement(&mut self, id: &NodeId) {
        self.replacements.retain(|c| &c.id != id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::ID_LEN;
    use std::net::{Ipv4Addr, SocketAddr};

    fn contact(n: u8) -> Contact {
        let mut bytes = [0u8; ID_LEN];
        bytes[ID_LEN - 1] = n;
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 4000 + n as u16));
        Contact::new(NodeId::from_bytes(bytes), addr)
    }

    fn ids(bucket: &KBucket) -> Vec<NodeId> {
        bucket.contacts().map(|c| c.id).collect()
    }

    #[test]
    fn inserts_until_full() {
        let mut bucket = KBucket::new(3);
        for n in 1..=3 {
            assert_eq!(bucket.update(contact(n)), UpdateOutcome::Inserted);
        }
        assert!(bucket.is_full());
        assert_eq!(bucket.len(), 3);
    }

    #[test]
    fn refresh_moves_contact_to_the_back() {
        let mut bucket = KBucket::new(3);
        for n in 1..=3 {
            bucket.update(contact(n));
        }
        assert_eq!(bucket.update(contact(1)), UpdateOutcome::Refreshed);
        assert_eq!(
            ids(&bucket),
            vec![contact(2).id, contact(3).id, contact(1).id]
        );
    }

    #[test]
    fn refresh_picks_up_a_new_address() {
        let mut bucket = KBucket::new(3);
        bucket.update(contact(1));
        let mut moved = contact(1);
        moved.addr.set_port(9999);
        bucket.update(moved);
        assert_eq!(bucket.contacts().next().unwrap().addr.port(), 9999);
    }

    #[test]
    fn full_bucket_keeps_old_contacts() {
        let mut bucket = KBucket::new(2);
        bucket.update(contact(1));
        bucket.update(contact(2));

        let outcome = bucket.update(contact(3));
        assert_eq!(
            outcome,
            UpdateOutcome::Full {
                least_recent: contact(1)
            }
        );
        assert_eq!(ids(&bucket), vec![contact(1).id, contact(2).id]);
        assert!(!bucket.contains(&contact(3).id));
    }

    #[test]
    fn removal_promotes_latest_replacement() {
        let mut bucket = KBucket::new(2);
        for n in 1..=4 {
            bucket.update(contact(n));
        }
        assert_eq!(bucket.remove(&contact(1).id), Some(contact(1)));
        assert_eq!(ids(&bucket), vec![contact(2).id, contact(4).id]);

        // 3 is still waiting and takes the next free slot.
        bucket.remove(&contact(2).id);
        assert_eq!(ids(&bucket), vec![contact(4).id, contact(3).id]);
    }

    #[test]
    fn replacement_cache_is_bounded() {
        let mut bucket = KBucket::new(2);
        for n in 1..=10 {
            bucket.update(contact(n));
        }
        assert_eq!(bucket.replacements.len(), 2);
        let cached: Vec<_> = bucket.replacements.iter().map(|c| c.id).collect();
        assert_eq!(cached, vec![contact(9).id, contact(10).id]);
    }

    #[test]
    fn unresponsive_contact_survives_without_a_replacement() {
        let mut bucket = KBucket::new(2);
        bucket.update(contact(1));
        bucket.update(contact(2));
        assert!(!bucket.mark_unresponsive(&contact(1).id));
        assert!(bucket.contains(&contact(1).id));

        bucket.update(contact(3));
        assert!(bucket.mark_unresponsive(&contact(1).id));
        assert_eq!(ids(&bucket), vec![contact(2).id, contact(3).id]);
    }

    #[test]
    fn freed_slot_takes_contact_out_of_replacements() {
        let mut bucket = KBucket::new(1);
        bucket.update(contact(1));
        bucket.update(contact(2));
        // Slot frees up without a promotion happening first.
        bucket.entries.clear();
        assert_eq!(bucket.update(contact(2)), UpdateOutcome::Inserted);
        assert!(bucket.replacements.is_empty());
    }
}
