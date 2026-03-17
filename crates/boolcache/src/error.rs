use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    #[error("ERR value is not a valid float")]
    NotFloat,

    #[error("ERR index out of range")]
    IndexOutOfRange,

    #[error("ERR no such key")]
    NoSuchKey,

    #[error("ERR syntax error")]
    Syntax,

    #[error("ERR {0}")]
    Other(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialize(String),
}

pub type Result<T> = std::result::Result<T, Error>;
