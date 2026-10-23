use std::time::Duration;

/// Tunable parameters for a node. The defaults follow the paper.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bucket size, and how many nodes each value gets stored on.
    pub k: usize,
    /// How many requests a lookup keeps in flight at once.
    pub alpha: usize,
    /// How long to wait for a reply before giving up on a peer.
    pub request_timeout: Duration,
    /// How long a stored value lives. This is also the most a peer can ask
    /// us to hold something for.
    pub record_ttl: Duration,
    /// The most records a node will hold for other people.
    pub max_records: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            k: 20,
            alpha: 3,
            request_timeout: Duration::from_secs(2),
            record_ttl: Duration::from_secs(24 * 60 * 60),
            max_records: 65_536,
        }
    }
}
