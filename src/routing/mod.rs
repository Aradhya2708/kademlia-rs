//! Routing state: k-buckets and the table that holds them.

mod bucket;
mod table;

pub use bucket::{KBucket, UpdateOutcome};
pub use table::RoutingTable;
