//! Error types for the Claude Agent SDK provider.

/// Errors that can occur when communicating via the Claude Agent SDK subprocess.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to spawn the Node.js subprocess.
    #[error("spawn error: {0}")]
    Spawn(std::io::Error),
    /// I/O error reading subprocess stdout.
    #[error("io error: {0}")]
    Io(std::io::Error),
    /// Failed to parse JSONL output from subprocess.
    #[error("json error: {0}")]
    Json(serde_json::Error),
    /// Subprocess wrote to stderr.
    #[error("subprocess error: {0}")]
    Stderr(String),
    /// No events received before process exited.
    #[error("no output from subprocess")]
    NoOutput,
    /// Timed out waiting for a chunk.
    #[error("stream timeout")]
    StreamTimeout,
    /// Message conversion error.
    #[error("convert error: {0}")]
    Convert(String),
}

impl objectiveai::error::StatusError for Error {
    fn status(&self) -> u16 {
        match self {
            Error::Spawn(_) => 500,
            Error::Io(_) => 500,
            Error::Json(_) => 500,
            Error::Stderr(_) => 502,
            Error::NoOutput => 502,
            Error::StreamTimeout => 504,
            Error::Convert(_) => 400,
        }
    }

    fn message(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "kind": "claude_agent_sdk_error",
            "message": self.to_string(),
        }))
    }
}
