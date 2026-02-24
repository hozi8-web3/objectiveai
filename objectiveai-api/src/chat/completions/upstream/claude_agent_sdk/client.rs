//! Claude Agent SDK client that spawns a Node.js subprocess for Anthropic models.

use std::time::Duration;

use futures::Stream;
use objectiveai::chat::completions::response::streaming::{
    ChatCompletionChunk, Choice, Delta, Object,
};
use objectiveai::chat::completions::response::{CostDetails, Usage};
use rust_decimal::Decimal;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

use super::event::{parse_event, ParsedEvent};

/// Claude Agent SDK client.
///
/// Unit struct — no configuration needed since it spawns a local subprocess.
#[derive(Debug, Clone)]
pub struct Client;

/// Transforms a model name for the Claude Agent SDK.
///
/// Strips the `anthropic/` prefix and replaces `.` with `-`.
fn transform_model(model: &str) -> String {
    let stripped = model.strip_prefix("anthropic/").unwrap_or(model);
    stripped.replace('.', "-")
}

impl Client {
    /// Creates a streaming chat completion via the Claude Agent SDK subprocess.
    pub fn create_streaming_for_chat(
        &self,
        id: String,
        _byok: Option<&str>,
        cost_multiplier: Decimal,
        first_chunk_timeout: Duration,
        other_chunk_timeout: Duration,
        ensemble_llm: &objectiveai::ensemble_llm::EnsembleLlm,
        request: &objectiveai::chat::completions::request::ChatCompletionCreateParams,
    ) -> impl Stream<Item = Result<ChatCompletionChunk, super::Error>> + Send + 'static {
        // Build merged messages using the shared prompt helper
        let messages = super::super::openrouter::request::prompt::new_for_chat(
            ensemble_llm.base.prefix_messages.as_deref(),
            &request.messages,
            ensemble_llm.base.suffix_messages.as_deref(),
        );

        let model = transform_model(&ensemble_llm.base.model);
        let ensemble_llm_id = ensemble_llm.id.clone();
        let verbosity = ensemble_llm
            .base
            .verbosity
            .map(|v| verbosity_to_str(v).to_owned());
        let reasoning_max_tokens = ensemble_llm
            .base
            .reasoning
            .and_then(|r| r.max_tokens);

        self.create_streaming_inner(
            id,
            ensemble_llm_id,
            cost_multiplier,
            first_chunk_timeout,
            other_chunk_timeout,
            messages,
            model,
            verbosity,
            reasoning_max_tokens,
            None, // no output_format for chat
        )
    }

    /// Creates a streaming chat completion for vector voting via the Claude Agent SDK.
    pub fn create_streaming_for_vector(
        &self,
        id: String,
        _byok: Option<&str>,
        cost_multiplier: Decimal,
        first_chunk_timeout: Duration,
        other_chunk_timeout: Duration,
        ensemble_llm: &objectiveai::ensemble_llm::EnsembleLlm,
        request: &objectiveai::vector::completions::request::VectorCompletionCreateParams,
        vector_pfx_indices: &[(String, usize)],
    ) -> impl Stream<Item = Result<ChatCompletionChunk, super::Error>> + Send + 'static {
        // Build merged messages using the shared vector prompt helper
        let messages = super::super::openrouter::request::prompt::new_for_vector(
            &request.responses,
            vector_pfx_indices,
            ensemble_llm.base.output_mode,
            ensemble_llm.base.prefix_messages.as_deref(),
            &request.messages,
            ensemble_llm.base.suffix_messages.as_deref(),
        );

        let model = transform_model(&ensemble_llm.base.model);
        let ensemble_llm_id = ensemble_llm.id.clone();
        let verbosity = ensemble_llm
            .base
            .verbosity
            .map(|v| verbosity_to_str(v).to_owned());
        let reasoning_max_tokens = ensemble_llm
            .base
            .reasoning
            .and_then(|r| r.max_tokens);

        // Build output_format for structured output modes
        let output_format_json = match ensemble_llm.base.output_mode {
            objectiveai::ensemble_llm::OutputMode::JsonSchema => {
                let think = ensemble_llm.base.synthetic_reasoning.unwrap_or(false);
                let keys: Vec<String> =
                    vector_pfx_indices.iter().map(|(k, _)| k.clone()).collect();
                let rf = crate::vector::completions::ResponseKey::response_format(
                    keys, think,
                );
                serde_json::to_string(&rf).ok()
            }
            objectiveai::ensemble_llm::OutputMode::ToolCall => {
                let think = ensemble_llm.base.synthetic_reasoning.unwrap_or(false);
                let keys: Vec<String> =
                    vector_pfx_indices.iter().map(|(k, _)| k.clone()).collect();
                let tool =
                    crate::vector::completions::ResponseKey::tool(keys, think);
                // Extract the function parameters as the output format
                let objectiveai::chat::completions::request::Tool::Function { function } = &tool;
                function
                    .parameters
                    .as_ref()
                    .and_then(|p| serde_json::to_string(&serde_json::Value::Object(p.clone())).ok())
            }
            objectiveai::ensemble_llm::OutputMode::Instruction => None,
        };

        self.create_streaming_inner(
            id,
            ensemble_llm_id,
            cost_multiplier,
            first_chunk_timeout,
            other_chunk_timeout,
            messages,
            model,
            verbosity,
            reasoning_max_tokens,
            output_format_json,
        )
    }

    /// Internal streaming implementation shared by chat and vector.
    fn create_streaming_inner(
        &self,
        id: String,
        ensemble_llm_id: String,
        cost_multiplier: Decimal,
        first_chunk_timeout: Duration,
        other_chunk_timeout: Duration,
        messages: Vec<objectiveai::chat::completions::request::Message>,
        model: String,
        verbosity: Option<String>,
        reasoning_max_tokens: Option<u64>,
        output_format_json: Option<String>,
    ) -> impl Stream<Item = Result<ChatCompletionChunk, super::Error>> + Send + 'static {
        async_stream::stream! {
            // Convert ObjectiveAI messages to SDK format
            let (system_prompt, sdk_message) =
                match super::convert::convert(&messages) {
                    Ok(result) => result,
                    Err(e) => {
                        yield Err(super::Error::Convert(e));
                        return;
                    }
                };

            let message_json = match serde_json::to_string(&sdk_message) {
                Ok(json) => json,
                Err(e) => {
                    yield Err(super::Error::Json(e));
                    return;
                }
            };

            // Build inline JS
            let js = super::js::build_js(
                &system_prompt,
                &message_json,
                &model,
                verbosity.as_deref(),
                reasoning_max_tokens,
                output_format_json.as_deref(),
            );

            // Spawn node subprocess
            let mut child = match Command::new("node")
                .arg("-e")
                .arg(&js)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
            {
                Ok(child) => child,
                Err(e) => {
                    yield Err(super::Error::Spawn(e));
                    return;
                }
            };

            // Collect stderr in background
            let stderr = child.stderr.take().expect("stderr was piped");
            let stderr_handle = tokio::spawn(async move {
                let mut buf = String::new();
                let mut reader = BufReader::new(stderr);
                let _ = tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut buf).await;
                buf
            });

            // Read stdout lines
            let stdout = child.stdout.take().expect("stdout was piped");
            let reader = BufReader::new(stdout);
            let mut lines_stream = LinesStream::new(reader.lines());

            // Wait for message_start event (with first_chunk_timeout)
            #[allow(unused_assignments)]
            let mut upstream_id = String::new();
            #[allow(unused_assignments)]
            let mut upstream_model = String::new();

            loop {
                match tokio::time::timeout(first_chunk_timeout, lines_stream.next()).await {
                    Err(_) => {
                        yield Err(super::Error::StreamTimeout);
                        return;
                    }
                    Ok(None) => {
                        let stderr_output = stderr_handle.await.unwrap_or_default();
                        if !stderr_output.is_empty() {
                            yield Err(super::Error::Stderr(stderr_output));
                        } else {
                            yield Err(super::Error::NoOutput);
                        }
                        return;
                    }
                    Ok(Some(Err(e))) => {
                        yield Err(super::Error::Io(e));
                        return;
                    }
                    Ok(Some(Ok(line))) => {
                        let line = line.trim().to_owned();
                        if line.is_empty() {
                            continue;
                        }

                        let value: serde_json::Value = match serde_json::from_str(&line) {
                            Ok(v) => v,
                            Err(e) => {
                                yield Err(super::Error::Json(e));
                                return;
                            }
                        };

                        if let Some(ParsedEvent::MessageStart { id: uid, model: umodel }) = parse_event(&value) {
                            upstream_id = uid;
                            upstream_model = umodel;
                            break;
                        }
                    }
                }
            }

            // Stream remaining events
            loop {
                match tokio::time::timeout(other_chunk_timeout, lines_stream.next()).await {
                    Err(_) => {
                        yield Err(super::Error::StreamTimeout);
                        return;
                    }
                    Ok(None) => {
                        // Process ended
                        return;
                    }
                    Ok(Some(Err(e))) => {
                        yield Err(super::Error::Io(e));
                        return;
                    }
                    Ok(Some(Ok(line))) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }

                        let value: serde_json::Value = match serde_json::from_str(trimmed) {
                            Ok(v) => v,
                            Err(e) => {
                                yield Err(super::Error::Json(e));
                                return;
                            }
                        };

                        match parse_event(&value) {
                            Some(ParsedEvent::TextDelta(text)) => {
                                yield Ok(make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    Delta { content: Some(text), ..Default::default() },
                                    None,
                                    None,
                                    None,
                                ));
                            }
                            Some(ParsedEvent::ThinkingDelta(thinking)) => {
                                yield Ok(make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    Delta { reasoning: Some(thinking), ..Default::default() },
                                    None,
                                    None,
                                    None,
                                ));
                            }
                            Some(ParsedEvent::MessageDelta { stop_reason, usage }) => {
                                yield Ok(make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    Delta::default(),
                                    stop_reason,
                                    usage,
                                    None,
                                ));
                            }
                            Some(ParsedEvent::MessageStop) => {
                                // Don't end yet — wait for the result event
                                continue;
                            }
                            Some(ParsedEvent::Result { total_cost_usd, service_tier }) => {
                                let cost_decimal = total_cost_usd
                                    .and_then(|c| Decimal::try_from(c).ok())
                                    .unwrap_or_default();
                                let adjusted_cost = cost_decimal * cost_multiplier;
                                let mut chunk = make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    Delta::default(),
                                    None,
                                    Some(Usage {
                                        cost: adjusted_cost,
                                        total_cost: adjusted_cost,
                                        cost_details: Some(CostDetails {
                                            upstream_inference_cost: cost_decimal,
                                            upstream_upstream_inference_cost: cost_decimal,
                                        }),
                                        ..Default::default()
                                    }),
                                    None,
                                );
                                chunk.service_tier = service_tier;
                                yield Ok(chunk);
                                return;
                            }
                            Some(ParsedEvent::MessageStart { .. }) | None => {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Creates a ChatCompletionChunk with the given parameters.
fn make_chunk(
    id: &str,
    upstream_id: &str,
    model: &str,
    upstream_model: &str,
    delta: Delta,
    finish_reason: Option<objectiveai::chat::completions::response::FinishReason>,
    usage: Option<Usage>,
    service_tier: Option<String>,
) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_owned(),
        upstream_id: upstream_id.to_owned(),
        model: model.to_owned(),
        upstream_model: upstream_model.to_owned(),
        created: 0,
        object: Object::default(),
        choices: vec![Choice {
            delta,
            finish_reason,
            index: 0,
            logprobs: None,
        }],
        service_tier,
        system_fingerprint: None,
        usage,
        provider: None,
        upstream: objectiveai::chat::completions::Upstream::ClaudeAgentSdk,
    }
}

/// Converts a Verbosity enum to its string representation for the SDK.
fn verbosity_to_str(v: objectiveai::ensemble_llm::Verbosity) -> &'static str {
    match v {
        objectiveai::ensemble_llm::Verbosity::Low => "low",
        objectiveai::ensemble_llm::Verbosity::Medium => "medium",
        objectiveai::ensemble_llm::Verbosity::High => "high",
        objectiveai::ensemble_llm::Verbosity::Max => "max",
    }
}
