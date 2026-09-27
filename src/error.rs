use std::{fmt, io};

#[derive(Debug)]
pub enum EngineError {
    Io(io::Error),
    Corruption(String),
    InvalidConfig(String),
    Locked,
    Background(String),
    Closed,
    SequenceExhausted,
}
impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Corruption(e) => write!(f, "corrupt storage: {e}"),
            Self::InvalidConfig(e) => write!(f, "invalid configuration: {e}"),
            Self::Locked => write!(f, "database is already open"),
            Self::Background(e) => {
                write!(f, "engine halted after I/O failure; reopen to recover: {e}")
            }
            Self::Closed => write!(f, "engine is closed"),
            Self::SequenceExhausted => write!(f, "sequence or file identifier exhausted"),
        }
    }
}
impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}
impl From<io::Error> for EngineError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
pub(crate) fn corrupt(msg: impl Into<String>) -> EngineError {
    EngineError::Corruption(msg.into())
}
