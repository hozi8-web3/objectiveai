//! Claude Agent SDK provider client and types.
//!
//! Spawns a Node.js subprocess using the `@anthropic-ai/claude-agent-sdk` package
//! to run Anthropic models, converting ObjectiveAI messages to the SDK format.

pub mod client;
mod content_block;
mod convert;
pub mod error;
mod js;
pub mod response;
mod sdk_message;

pub use error::Error;
