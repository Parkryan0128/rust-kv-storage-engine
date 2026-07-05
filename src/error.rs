use std::fmt;

#[derive(Debug)]
pub struct EngineError;

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "engine error")
    }
}

impl std::error::Error for EngineError {}
