//! Remote source types for function and profile hosting.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The remote source where a function or profile is hosted.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Remote {
    /// GitHub repository.
    Github,
    /// Local filesystem.
    Filesystem,
}

impl fmt::Display for Remote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Remote::Github => write!(f, "github"),
            Remote::Filesystem => write!(f, "filesystem"),
        }
    }
}

impl std::str::FromStr for Remote {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "github" => Ok(Remote::Github),
            "filesystem" => Ok(Remote::Filesystem),
            _ => Err(format!("invalid remote: {}", s)),
        }
    }
}
