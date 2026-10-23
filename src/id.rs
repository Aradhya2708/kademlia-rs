//! Identifiers in the 160-bit Kademlia keyspace and the XOR metric over them.
//!
//! Node ids and keys live in the same space. That is what makes a node
//! "responsible" for a key: it stores the keys that are closest to its own id.

use std::fmt;

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

/// Length of an id in bytes.
pub const ID_LEN: usize = 20;

/// Length of an id in bits.
pub const ID_BITS: usize = ID_LEN * 8;

/// A 160-bit identifier for a node or a key.
///
/// The bytes are treated as a big-endian unsigned integer, so the derived
/// `Ord` matches numeric order.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId([u8; ID_LEN]);

impl NodeId {
    pub const fn from_bytes(bytes: [u8; ID_LEN]) -> Self {
        Self(bytes)
    }

    /// Generates a uniformly random id.
    pub fn random() -> Self {
        Self(rand::random())
    }

    /// Maps arbitrary data into the keyspace by hashing it with SHA-1, the
    /// same hash the original paper uses.
    pub fn hash(data: &[u8]) -> Self {
        let digest = Sha1::digest(data);
        let mut bytes = [0u8; ID_LEN];
        bytes.copy_from_slice(&digest);
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; ID_LEN] {
        &self.0
    }

    /// XOR distance between two ids.
    pub fn distance(&self, other: &NodeId) -> Distance {
        let mut out = [0u8; ID_LEN];
        for (o, (a, b)) in out.iter_mut().zip(self.0.iter().zip(&other.0)) {
            *o = a ^ b;
        }
        Distance(out)
    }

    /// Returns a random id that falls in bucket `index` relative to `self`.
    ///
    /// Refreshing a bucket comes down to looking up some id in its range, and
    /// this is how we pick one.
    pub fn random_in_bucket(&self, index: usize) -> NodeId {
        assert!(index < ID_BITS, "bucket index {index} out of range");

        // Build a random distance whose highest set bit is `index`: clear
        // everything above it, set it, and leave the lower bits random.
        let mut distance: [u8; ID_LEN] = rand::random();
        let byte = ID_LEN - 1 - index / 8;
        let bit = index % 8;
        distance[..byte].fill(0);
        distance[byte] = (distance[byte] & ((1 << bit) - 1)) | (1 << bit);

        let mut out = self.0;
        for (o, d) in out.iter_mut().zip(&distance) {
            *o ^= d;
        }
        NodeId(out)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(f, &self.0)
    }
}

impl fmt::Debug for NodeId {
    // Full ids are 40 hex characters, which makes logs painful to read. The
    // first four bytes are plenty to tell nodes apart.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeId(")?;
        write_hex(f, &self.0[..4])?;
        f.write_str("..)")
    }
}

/// The XOR distance between two ids, ordered as a 160-bit unsigned integer.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Distance([u8; ID_LEN]);

impl Distance {
    pub const ZERO: Distance = Distance([0; ID_LEN]);
    pub const MAX: Distance = Distance([0xff; ID_LEN]);

    pub fn as_bytes(&self) -> &[u8; ID_LEN] {
        &self.0
    }

    /// Number of leading zero bits, which is also the length of the prefix
    /// the two ids have in common.
    pub fn leading_zeros(&self) -> u32 {
        let mut zeros = 0;
        for &byte in &self.0 {
            if byte != 0 {
                return zeros + byte.leading_zeros();
            }
            zeros += 8;
        }
        zeros
    }

    /// Index of the k-bucket that a node at this distance belongs in.
    ///
    /// Bucket `i` covers distances in `[2^i, 2^(i+1))`, so the index is the
    /// position of the highest set bit. A zero distance means the id is our
    /// own, and we never put ourselves in a bucket.
    pub fn bucket_index(&self) -> Option<usize> {
        let zeros = self.leading_zeros() as usize;
        (zeros < ID_BITS).then(|| ID_BITS - 1 - zeros)
    }
}

impl fmt::Debug for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Distance(")?;
        write_hex(f, &self.0)?;
        f.write_str(")")
    }
}

fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_with_last_byte(b: u8) -> NodeId {
        let mut bytes = [0u8; ID_LEN];
        bytes[ID_LEN - 1] = b;
        NodeId::from_bytes(bytes)
    }

    #[test]
    fn distance_to_self_is_zero() {
        let id = NodeId::random();
        assert_eq!(id.distance(&id), Distance::ZERO);
    }

    #[test]
    fn distance_is_symmetric() {
        for _ in 0..100 {
            let a = NodeId::random();
            let b = NodeId::random();
            assert_eq!(a.distance(&b), b.distance(&a));
        }
    }

    #[test]
    fn distances_compose_with_xor() {
        // d(a, c) = d(a, b) ^ d(b, c). This is what gives the metric its
        // triangle inequality, since x ^ y <= x + y.
        for _ in 0..100 {
            let (a, b, c) = (NodeId::random(), NodeId::random(), NodeId::random());
            let ab = NodeId::from_bytes(*a.distance(&b).as_bytes());
            let bc = NodeId::from_bytes(*b.distance(&c).as_bytes());
            assert_eq!(ab.distance(&bc), a.distance(&c));
        }
    }

    #[test]
    fn distances_order_numerically() {
        let origin = NodeId::from_bytes([0; ID_LEN]);
        let mut high = [0u8; ID_LEN];
        high[0] = 1;
        let far = origin.distance(&NodeId::from_bytes(high));
        let near = origin.distance(&id_with_last_byte(0xff));
        assert!(near < far);
        assert!(Distance::ZERO < near);
        assert!(far < Distance::MAX);
    }

    #[test]
    fn leading_zeros_counts_common_prefix() {
        assert_eq!(Distance::ZERO.leading_zeros(), ID_BITS as u32);
        assert_eq!(Distance::MAX.leading_zeros(), 0);

        let origin = NodeId::from_bytes([0; ID_LEN]);
        assert_eq!(origin.distance(&id_with_last_byte(1)).leading_zeros(), 159);
        assert_eq!(
            origin.distance(&id_with_last_byte(0b1000)).leading_zeros(),
            156
        );
    }

    #[test]
    fn bucket_index_is_position_of_highest_bit() {
        let origin = NodeId::from_bytes([0; ID_LEN]);
        assert_eq!(Distance::ZERO.bucket_index(), None);
        assert_eq!(Distance::MAX.bucket_index(), Some(159));
        assert_eq!(
            origin.distance(&id_with_last_byte(1)).bucket_index(),
            Some(0)
        );
        assert_eq!(
            origin.distance(&id_with_last_byte(0b101)).bucket_index(),
            Some(2)
        );
    }

    #[test]
    fn random_in_bucket_lands_in_that_bucket() {
        let id = NodeId::random();
        for index in 0..ID_BITS {
            for _ in 0..5 {
                let other = id.random_in_bucket(index);
                assert_eq!(id.distance(&other).bucket_index(), Some(index));
            }
        }
    }

    #[test]
    fn hash_matches_sha1() {
        let id = NodeId::hash(b"hello");
        assert_eq!(id.to_string(), "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
    }

    #[test]
    fn debug_output_is_abbreviated() {
        let id = NodeId::hash(b"hello");
        assert_eq!(format!("{id:?}"), "NodeId(aaf4c61d..)");
    }
}
