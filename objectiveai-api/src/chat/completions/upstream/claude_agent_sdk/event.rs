//! Event parsing for Claude Agent SDK subprocess JSONL output.

use objectiveai::chat::completions::response::{FinishReason, PromptTokensDetails, Usage};

/// Parsed event from the subprocess JSONL stream.
pub enum ParsedEvent {
    /// Initial message with upstream IDs.
    MessageStart { id: String, model: String },
    /// Text content delta.
    TextDelta(String),
    /// Thinking/reasoning content delta.
    ThinkingDelta(String),
    /// Message completion with optional stop reason and usage.
    MessageDelta {
        stop_reason: Option<FinishReason>,
        usage: Option<Usage>,
    },
    /// Message stop marker.
    MessageStop,
    /// Final result with cost and service tier.
    Result {
        total_cost_usd: Option<f64>,
        service_tier: Option<String>,
    },
}

/// Parses a JSON value from the subprocess into a typed event.
pub fn parse_event(value: &serde_json::Value) -> Option<ParsedEvent> {
    let event_type = value.get("type")?.as_str()?;
    match event_type {
        "stream_event" => {
            let event = value.get("event")?;
            let sub_type = event.get("type")?.as_str()?;
            match sub_type {
                "message_start" => {
                    let msg = event.get("message")?;
                    let id = msg.get("id")?.as_str()?.to_owned();
                    let model = msg.get("model")?.as_str()?.to_owned();
                    Some(ParsedEvent::MessageStart { id, model })
                }
                "content_block_delta" => {
                    let delta = event.get("delta")?;
                    let delta_type = delta.get("type")?.as_str()?;
                    match delta_type {
                        "text_delta" => {
                            let text = delta.get("text")?.as_str()?.to_owned();
                            Some(ParsedEvent::TextDelta(text))
                        }
                        "thinking_delta" => {
                            let thinking = delta.get("thinking")?.as_str()?.to_owned();
                            Some(ParsedEvent::ThinkingDelta(thinking))
                        }
                        _ => None,
                    }
                }
                "message_delta" => {
                    let delta = event.get("delta")?;
                    let stop_reason = delta
                        .get("stop_reason")
                        .and_then(|v| v.as_str())
                        .map(|s| match s {
                            "end_turn" | "stop" => FinishReason::Stop,
                            "max_tokens" | "length" => FinishReason::Length,
                            "tool_use" | "tool_calls" => FinishReason::ToolCalls,
                            "content_filter" => FinishReason::ContentFilter,
                            _ => FinishReason::Error,
                        });
                    let usage = event.get("usage").and_then(|u| {
                        let input = u.get("input_tokens")?.as_u64()?;
                        let output = u.get("output_tokens")?.as_u64()?;
                        let cache_creation = u
                            .get("cache_creation_input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let cache_read = u
                            .get("cache_read_input_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let prompt_tokens = input + cache_creation + cache_read;
                        Some(Usage {
                            prompt_tokens,
                            completion_tokens: output,
                            total_tokens: prompt_tokens + output,
                            prompt_tokens_details: Some(PromptTokensDetails {
                                cached_tokens: Some(cache_read),
                                cache_write_tokens: Some(cache_creation),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                    });
                    Some(ParsedEvent::MessageDelta { stop_reason, usage })
                }
                "message_stop" => Some(ParsedEvent::MessageStop),
                _ => None,
            }
        }
        "result" => {
            let total_cost_usd = value.get("total_cost_usd").and_then(|v| v.as_f64());
            let service_tier = value
                .get("usage")
                .and_then(|u| u.get("service_tier"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned());
            Some(ParsedEvent::Result {
                total_cost_usd,
                service_tier,
            })
        }
        _ => None,
    }
}
