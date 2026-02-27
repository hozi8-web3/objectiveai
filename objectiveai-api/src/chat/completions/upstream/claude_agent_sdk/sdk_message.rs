//! SDK user message types for the Claude Agent SDK subprocess.

use serde::{Deserialize, Serialize};

use super::content_block::ContentBlockParam;

// --- MessageParam ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MessageRole {
    #[serde(rename = "user")]
    User,
    #[serde(rename = "assistant")]
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlockParam>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageParam {
    pub content: MessageContent,
    pub role: MessageRole,
}

// --- SDKUserMessage ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SDKUserMessage {
    pub r#type: SDKUserMessageType,
    pub message: MessageParam,
    pub parent_tool_use_id: Option<String>,
    #[serde(rename = "isSynthetic")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_synthetic: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_use_result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SDKUserMessageType {
    #[serde(rename = "user")]
    User,
}
