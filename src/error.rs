use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("malformed message: {0}")]
    Codec(#[from] postcard::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
