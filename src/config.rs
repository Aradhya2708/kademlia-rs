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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            k: 20,
            alpha: 3,
            request_timeout: Duration::from_secs(2),
        }
    }
}
