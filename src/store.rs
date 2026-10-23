//! The records a node holds on behalf of the network.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::id::NodeId;

#[derive(Debug, Clone)]
pub(crate) struct Record {
    pub value: Vec<u8>,
    pub expires_at: Instant,
    /// When this record was last stored here or republished from here.
    refreshed_at: Instant,
}

impl Record {
    /// How much longer the record has to live.
    pub fn ttl(&self, now: Instant) -> Duration {
        self.expires_at.saturating_duration_since(now)
    }
}

/// An in-memory map from key to record, with a cap on how many it will hold.
///
/// Everything takes `now` as an argument instead of reading the clock, which
/// keeps the expiry logic easy to test.
#[derive(Debug)]
pub(crate) struct RecordStore {
    records: HashMap<NodeId, Record>,
    max_records: usize,
}

impl RecordStore {
    pub fn new(max_records: usize) -> Self {
        Self {
            records: HashMap::new(),
            max_records,
        }
    }

    /// Stores `value` under `key` for `ttl`, replacing whatever was there.
    ///
    /// Returns `false` if the store is full and `key` is new. Updates to keys
    /// we already hold are always accepted.
    pub fn put(&mut self, key: NodeId, value: Vec<u8>, ttl: Duration, now: Instant) -> bool {
        if !self.records.contains_key(&key) && self.records.len() >= self.max_records {
            // Expired records may still be holding slots until the next sweep.
            self.remove_expired(now);
            if self.records.len() >= self.max_records {
                return false;
            }
        }

        let expires_at = now
            .checked_add(ttl)
            .expect("ttl should be clamped before it reaches the store");
        self.records.insert(
            key,
            Record {
                value,
                expires_at,
                refreshed_at: now,
            },
        );
        true
    }

    pub fn get(&self, key: &NodeId, now: Instant) -> Option<&Record> {
        self.records.get(key).filter(|r| r.expires_at > now)
    }

    /// Drops every record that has expired, and returns how many there were.
    pub fn remove_expired(&mut self, now: Instant) -> usize {
        let before = self.records.len();
        self.records.retain(|_, r| r.expires_at > now);
        before - self.records.len()
    }

    /// Records that are due to be pushed back out to the network, as
    /// `(key, value, remaining ttl)`. They're marked as refreshed on the way
    /// out.
    ///
    /// A record is due once nobody has stored it here, and we haven't
    /// republished it, for a whole `interval`. That's the optimisation from
    /// section 2.5 of the paper: if another node just republished a record to
    /// us, the other k-1 nodes holding it almost certainly got it too, so
    /// there's no point in us doing the same thing an hour later.
    pub fn take_due_for_republish(
        &mut self,
        now: Instant,
        interval: Duration,
    ) -> Vec<(NodeId, Vec<u8>, Duration)> {
        self.records
            .iter_mut()
            .filter(|(_, r)| r.expires_at > now && now.duration_since(r.refreshed_at) >= interval)
            .map(|(key, r)| {
                r.refreshed_at = now;
                (*key, r.value.clone(), r.ttl(now))
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);

    #[test]
    fn stored_values_can_be_read_back() {
        let mut store = RecordStore::new(10);
        let now = Instant::now();
        let key = NodeId::hash(b"key");
        assert!(store.put(key, b"value".to_vec(), HOUR, now));

        let record = store.get(&key, now).unwrap();
        assert_eq!(record.value, b"value");
        assert_eq!(record.ttl(now), HOUR);
        assert!(store.get(&NodeId::hash(b"other"), now).is_none());
    }

    #[test]
    fn newer_value_replaces_older() {
        let mut store = RecordStore::new(10);
        let now = Instant::now();
        let key = NodeId::hash(b"key");
        store.put(key, b"old".to_vec(), HOUR, now);
        store.put(key, b"new".to_vec(), HOUR, now);
        assert_eq!(store.get(&key, now).unwrap().value, b"new");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn expired_records_disappear() {
        let mut store = RecordStore::new(10);
        let now = Instant::now();
        let key = NodeId::hash(b"key");
        store.put(key, b"value".to_vec(), HOUR, now);

        let later = now + HOUR;
        assert!(store.get(&key, later).is_none());
        assert_eq!(store.len(), 1, "not swept yet");
        assert_eq!(store.remove_expired(later), 1);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn full_store_turns_away_new_keys_only() {
        let mut store = RecordStore::new(2);
        let now = Instant::now();
        let (a, b, c) = (NodeId::hash(b"a"), NodeId::hash(b"b"), NodeId::hash(b"c"));
        assert!(store.put(a, vec![1], HOUR, now));
        assert!(store.put(b, vec![2], HOUR, now));
        assert!(!store.put(c, vec![3], HOUR, now));
        assert!(store.put(a, vec![4], HOUR, now));
    }

    #[test]
    fn republish_waits_a_full_interval() {
        let mut store = RecordStore::new(10);
        let now = Instant::now();
        let key = NodeId::hash(b"key");
        store.put(key, b"value".to_vec(), 24 * HOUR, now);

        assert!(
            store
                .take_due_for_republish(now + HOUR / 2, HOUR)
                .is_empty()
        );

        let due = store.take_due_for_republish(now + HOUR, HOUR);
        assert_eq!(due, vec![(key, b"value".to_vec(), 23 * HOUR)]);

        // Taken once, not due again until another interval has passed.
        assert!(store.take_due_for_republish(now + HOUR, HOUR).is_empty());
        assert_eq!(store.take_due_for_republish(now + 2 * HOUR, HOUR).len(), 1);
    }

    #[test]
    fn incoming_store_pushes_back_republish() {
        let mut store = RecordStore::new(10);
        let now = Instant::now();
        let key = NodeId::hash(b"key");
        store.put(key, b"value".to_vec(), 24 * HOUR, now);
        // Someone else republished it to us half an hour later.
        store.put(key, b"value".to_vec(), 24 * HOUR, now + HOUR / 2);

        assert!(store.take_due_for_republish(now + HOUR, HOUR).is_empty());
    }

    #[test]
    fn expired_records_are_never_republished() {
        let mut store = RecordStore::new(10);
        let now = Instant::now();
        store.put(NodeId::hash(b"key"), vec![1], HOUR, now);
        assert!(
            store
                .take_due_for_republish(now + 2 * HOUR, HOUR)
                .is_empty()
        );
    }

    #[test]
    fn full_store_makes_room_by_dropping_expired_records() {
        let mut store = RecordStore::new(1);
        let now = Instant::now();
        store.put(NodeId::hash(b"a"), vec![1], Duration::from_secs(1), now);

        let later = now + Duration::from_secs(2);
        assert!(store.put(NodeId::hash(b"b"), vec![2], HOUR, later));
        assert_eq!(store.len(), 1);
    }
}
