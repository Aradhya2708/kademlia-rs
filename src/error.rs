use std::io;
use std::net::SocketAddr;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("malformed message: {0}")]
    Codec(#[from] postcard::Error),

    #[error("no response from {0}")]
    Timeout(SocketAddr),

    #[error("peer sent a response that doesn't match the request")]
    UnexpectedResponse,

    #[error("value is {0} bytes, more than the {max} byte limit", max = crate::protocol::MAX_VALUE_LEN)]
    ValueTooLarge(usize),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
