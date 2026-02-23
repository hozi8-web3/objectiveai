//! Upstream provider enumeration.

use serde::{Deserialize, Serialize};

/// Supported upstream LLM providers.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Upstream {
    /// Unknown provider.
    #[default]
    Unknown,
    /// OpenRouter provider.
    OpenRouter,
    /// Claude Agent SDK provider.
    ClaudeAgentSdk,
}
