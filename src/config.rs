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
    /// How often a node pushes the records it holds back out to the `k`
    /// closest nodes. This keeps values alive as nodes come and go. Expired
    /// records are swept out on the same schedule.
    pub republish_interval: Duration,
    /// Buckets that go this long without a lookup get refreshed with one.
    pub refresh_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        const HOUR: Duration = Duration::from_secs(60 * 60);
        Self {
            k: 20,
            alpha: 3,
            request_timeout: Duration::from_secs(2),
            record_ttl: 24 * HOUR,
            max_records: 65_536,
            republish_interval: HOUR,
            refresh_interval: HOUR,
        }
    }
}
