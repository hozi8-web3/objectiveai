//! Typed event deserialization for Claude Agent SDK subprocess JSONL output.

use objectiveai::chat::completions::response::FinishReason;
use serde::Deserialize;

/// Parsed event from the subprocess JSONL stream.
pub enum ParsedEvent {
    /// Initial message with upstream IDs.
    MessageStart { id: String, model: String },
    /// Text content delta.
    TextDelta(String),
    /// Thinking/reasoning content delta.
    ThinkingDelta(String),
    /// Message completion with optional stop reason and token usage.
    MessageDelta {
        stop_reason: Option<FinishReason>,
        usage: Option<AnthropicUsage>,
    },
    /// Message stop marker.
    MessageStop,
    /// Final result with cost and service tier.
    Result {
        total_cost_usd: Option<f64>,
        service_tier: Option<String>,
    },
}

/// Token usage from Anthropic's message_delta event.
#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}

/// Parses a JSONL line from the subprocess into a typed event.
///
/// Returns `Ok(None)` for unrecognized event types (forward-compatible).
/// Returns `Err` for malformed JSON.
pub fn parse_line(
    line: &str,
) -> Result<Option<ParsedEvent>, serde_json::Error> {
    let envelope: Envelope = serde_json::from_str(line)?;
    Ok(match envelope {
        Envelope::StreamEvent { event } => match event {
            StreamEvent::MessageStart { message } => {
                Some(ParsedEvent::MessageStart {
                    id: message.id,
                    model: message.model,
                })
            }
            StreamEvent::ContentBlockDelta { delta } => match delta {
                ContentDelta::TextDelta { text } => {
                    Some(ParsedEvent::TextDelta(text))
                }
                ContentDelta::ThinkingDelta { thinking } => {
                    Some(ParsedEvent::ThinkingDelta(thinking))
                }
                ContentDelta::Unknown => None,
            },
            StreamEvent::MessageDelta { delta, usage } => {
                Some(ParsedEvent::MessageDelta {
                    stop_reason: delta
                        .stop_reason
                        .as_deref()
                        .map(parse_stop_reason),
                    usage,
                })
            }
            StreamEvent::MessageStop => Some(ParsedEvent::MessageStop),
            StreamEvent::Unknown => None,
        },
        Envelope::Result(result) => Some(ParsedEvent::Result {
            total_cost_usd: result.total_cost_usd,
            service_tier: result.usage.and_then(|u| u.service_tier),
        }),
        Envelope::Unknown => None,
    })
}

// ---- Serde envelope types (private) ----

/// Top-level envelope from the subprocess JSONL stream.
#[derive(Deserialize)]
#[serde(tag = "type")]
enum Envelope {
    #[serde(rename = "stream_event")]
    StreamEvent { event: StreamEvent },
    #[serde(rename = "result")]
    Result(ResultEvent),
    #[serde(other)]
    Unknown,
}

/// Anthropic stream event types.
#[derive(Deserialize)]
#[serde(tag = "type")]
enum StreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: MessageStartMessage },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { delta: ContentDelta },
    #[serde(rename = "message_delta")]
    MessageDelta {
        delta: MessageDeltaBody,
        usage: Option<AnthropicUsage>,
    },
    #[serde(rename = "message_stop")]
    MessageStop,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct MessageStartMessage {
    id: String,
    model: String,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ContentDelta {
    #[serde(rename = "text_delta")]
    TextDelta { text: String },
    #[serde(rename = "thinking_delta")]
    ThinkingDelta { thinking: String },
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct MessageDeltaBody {
    stop_reason: Option<String>,
}

#[derive(Deserialize)]
struct ResultEvent {
    total_cost_usd: Option<f64>,
    usage: Option<ResultUsage>,
}

#[derive(Deserialize)]
struct ResultUsage {
    service_tier: Option<String>,
}

fn parse_stop_reason(s: &str) -> FinishReason {
    match s {
        "end_turn" | "stop" => FinishReason::Stop,
        "max_tokens" | "length" => FinishReason::Length,
        "tool_use" | "tool_calls" => FinishReason::ToolCalls,
        "content_filter" => FinishReason::ContentFilter,
        _ => FinishReason::Error,
    }
}
