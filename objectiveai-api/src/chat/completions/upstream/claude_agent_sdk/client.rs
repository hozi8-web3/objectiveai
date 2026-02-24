//! Claude Agent SDK client that spawns a Node.js subprocess for Anthropic models.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::Stream;
use objectiveai::chat::completions::response::streaming::{
    ChatCompletionChunk, Choice, Delta, Object,
};
use rust_decimal::Decimal;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::LinesStream;

use super::response::event::{self, AnthropicUsage, ParsedEvent};

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

/// State captured from the init phase (message_start event).
struct InitResult {
    upstream_id: String,
    upstream_model: String,
    lines_stream: LinesStream<BufReader<tokio::process::ChildStdout>>,
}

/// Builds the output_format JSON string for structured output modes.
fn output_format_json(
    ensemble_llm: &objectiveai::ensemble_llm::EnsembleLlm,
    vector_pfx_indices: &[(String, usize)],
) -> Option<String> {
    match ensemble_llm.base.output_mode {
        objectiveai::ensemble_llm::OutputMode::JsonSchema => {
            let think = ensemble_llm.base.synthetic_reasoning.unwrap_or(false);
            let keys: Vec<String> =
                vector_pfx_indices.iter().map(|(k, _)| k.clone()).collect();
            let rf = crate::vector::completions::ResponseKey::response_format(
                keys, think,
            );
            Some(serde_json::to_string(&rf).unwrap())
        }
        objectiveai::ensemble_llm::OutputMode::ToolCall => {
            let think = ensemble_llm.base.synthetic_reasoning.unwrap_or(false);
            let keys: Vec<String> =
                vector_pfx_indices.iter().map(|(k, _)| k.clone()).collect();
            let tool =
                crate::vector::completions::ResponseKey::tool(keys, think);
            let objectiveai::chat::completions::request::Tool::Function {
                function,
            } = &tool;
            function.parameters.as_ref().map(|p| {
                serde_json::to_string(&serde_json::Value::Object(p.clone()))
                    .unwrap()
            })
        }
        objectiveai::ensemble_llm::OutputMode::Instruction => None,
    }
}

/// Returns the current Unix timestamp in seconds.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
    ) -> impl Stream<Item = Result<ChatCompletionChunk, super::Error>> + Send + 'static
    {
        let messages = super::super::openrouter::request::prompt::new_for_chat(
            ensemble_llm.base.prefix_messages.as_deref(),
            &request.messages,
            ensemble_llm.base.suffix_messages.as_deref(),
        );

        let model = transform_model(&ensemble_llm.base.model);
        let ensemble_llm_id = ensemble_llm.id.clone();
        let is_byok = _byok.is_some();
        let verbosity = ensemble_llm
            .base
            .verbosity
            .map(|v| verbosity_to_str(v).to_owned());
        let reasoning_max_tokens =
            ensemble_llm.base.reasoning.and_then(|r| r.max_tokens);

        self.create_streaming_inner(
            id,
            ensemble_llm_id,
            is_byok,
            cost_multiplier,
            first_chunk_timeout,
            other_chunk_timeout,
            messages,
            model,
            verbosity,
            reasoning_max_tokens,
            None,
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
    ) -> impl Stream<Item = Result<ChatCompletionChunk, super::Error>> + Send + 'static
    {
        let messages =
            super::super::openrouter::request::prompt::new_for_vector(
                &request.responses,
                vector_pfx_indices,
                ensemble_llm.base.output_mode,
                ensemble_llm.base.prefix_messages.as_deref(),
                &request.messages,
                ensemble_llm.base.suffix_messages.as_deref(),
            );

        let model = transform_model(&ensemble_llm.base.model);
        let ensemble_llm_id = ensemble_llm.id.clone();
        let is_byok = _byok.is_some();
        let verbosity = ensemble_llm
            .base
            .verbosity
            .map(|v| verbosity_to_str(v).to_owned());
        let reasoning_max_tokens =
            ensemble_llm.base.reasoning.and_then(|r| r.max_tokens);
        let fmt = output_format_json(ensemble_llm, vector_pfx_indices);

        self.create_streaming_inner(
            id,
            ensemble_llm_id,
            is_byok,
            cost_multiplier,
            first_chunk_timeout,
            other_chunk_timeout,
            messages,
            model,
            verbosity,
            reasoning_max_tokens,
            fmt,
        )
    }

    /// Internal streaming implementation shared by chat and vector.
    #[allow(clippy::too_many_arguments)]
    fn create_streaming_inner(
        &self,
        id: String,
        ensemble_llm_id: String,
        is_byok: bool,
        cost_multiplier: Decimal,
        first_chunk_timeout: Duration,
        other_chunk_timeout: Duration,
        messages: Vec<objectiveai::chat::completions::request::Message>,
        model: String,
        verbosity: Option<String>,
        reasoning_max_tokens: Option<u64>,
        output_format_json: Option<String>,
    ) -> impl Stream<Item = Result<ChatCompletionChunk, super::Error>> + Send + 'static
    {
        async_stream::stream! {
            // Convert ObjectiveAI messages to SDK format
            let (system_prompt, sdk_message) = match super::convert::convert(&messages) {
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
            let lines_stream = LinesStream::new(reader.lines());

            let created = now_unix();

            // Wait for message_start event
            let init = match wait_for_init(lines_stream, first_chunk_timeout, &stderr_handle).await {
                Ok(init) => init,
                Err(e) => {
                    let _ = child.kill().await;
                    yield Err(e);
                    return;
                }
            };

            let upstream_id = init.upstream_id;
            let upstream_model = init.upstream_model;
            let mut lines_stream = init.lines_stream;

            // Track accumulated token usage from MessageDelta events
            let mut last_usage: Option<AnthropicUsage> = None;

            // Stream remaining events
            loop {
                match tokio::time::timeout(other_chunk_timeout, lines_stream.next()).await {
                    Err(_) => {
                        let _ = child.kill().await;
                        // Try to collect stderr for context
                        let stderr_ctx = tokio::time::timeout(
                            Duration::from_secs(2),
                            stderr_handle,
                        )
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .unwrap_or_default();
                        if stderr_ctx.is_empty() {
                            yield Err(super::Error::StreamTimeout);
                        } else {
                            yield Err(super::Error::Stderr(
                                format!("stream timeout; stderr: {stderr_ctx}"),
                            ));
                        }
                        return;
                    }
                    Ok(None) => {
                        // Process ended
                        return;
                    }
                    Ok(Some(Err(e))) => {
                        let _ = child.kill().await;
                        yield Err(super::Error::Io(e));
                        return;
                    }
                    Ok(Some(Ok(line))) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }

                        let parsed = match event::parse_line(trimmed) {
                            Ok(Some(event)) => event,
                            Ok(None) => continue,
                            Err(e) => {
                                let _ = child.kill().await;
                                yield Err(super::Error::Json(e));
                                return;
                            }
                        };

                        match parsed {
                            ParsedEvent::TextDelta(text) => {
                                yield Ok(make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    created,
                                    Delta { content: Some(text), ..Default::default() },
                                    None,
                                    None,
                                    None,
                                ));
                            }
                            ParsedEvent::ThinkingDelta(thinking) => {
                                yield Ok(make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    created,
                                    Delta { reasoning: Some(thinking), ..Default::default() },
                                    None,
                                    None,
                                    None,
                                ));
                            }
                            ParsedEvent::MessageDelta { stop_reason, usage } => {
                                // Accumulate usage for merging with the Result event
                                if usage.is_some() {
                                    last_usage = usage;
                                }
                                yield Ok(make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    created,
                                    Delta::default(),
                                    stop_reason,
                                    None,
                                    None,
                                ));
                            }
                            ParsedEvent::MessageStop => {
                                // Wait for the Result event
                                continue;
                            }
                            ParsedEvent::Result { total_cost_usd, service_tier } => {
                                let cost = total_cost_usd
                                    .and_then(|c| Decimal::try_from(c).ok())
                                    .unwrap_or_default();

                                // Merge accumulated token usage with cost
                                let usage = last_usage
                                    .take()
                                    .map(|u| u.into_downstream(cost, is_byok, cost_multiplier))
                                    .unwrap_or_else(|| {
                                        // No token usage available — emit cost-only usage
                                        AnthropicUsage {
                                            input_tokens: 0,
                                            output_tokens: 0,
                                            cache_creation_input_tokens: 0,
                                            cache_read_input_tokens: 0,
                                        }
                                        .into_downstream(cost, is_byok, cost_multiplier)
                                    });

                                let mut chunk = make_chunk(
                                    &id,
                                    &upstream_id,
                                    &ensemble_llm_id,
                                    &upstream_model,
                                    created,
                                    Delta::default(),
                                    None,
                                    Some(usage),
                                    None,
                                );
                                chunk.service_tier = service_tier;
                                yield Ok(chunk);
                                return;
                            }
                            ParsedEvent::MessageStart { .. } => {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Reads lines until a `message_start` event is received.
async fn wait_for_init(
    mut lines_stream: LinesStream<BufReader<tokio::process::ChildStdout>>,
    timeout: Duration,
    stderr_handle: &tokio::task::JoinHandle<String>,
) -> Result<InitResult, super::Error> {
    loop {
        match tokio::time::timeout(timeout, lines_stream.next()).await {
            Err(_) => return Err(super::Error::StreamTimeout),
            Ok(None) => {
                // Process ended before message_start — check stderr
                // We can't await the handle since we only have a reference,
                // but we can check if stderr has finished
                if stderr_handle.is_finished() {
                    // Safety: we just checked is_finished
                    // But we can't consume the handle. Just report NoOutput.
                }
                return Err(super::Error::NoOutput);
            }
            Ok(Some(Err(e))) => return Err(super::Error::Io(e)),
            Ok(Some(Ok(line))) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                match event::parse_line(trimmed) {
                    Ok(Some(ParsedEvent::MessageStart { id, model })) => {
                        return Ok(InitResult {
                            upstream_id: id,
                            upstream_model: model,
                            lines_stream,
                        });
                    }
                    Ok(_) => continue,
                    Err(e) => return Err(super::Error::Json(e)),
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
    created: u64,
    delta: Delta,
    finish_reason: Option<
        objectiveai::chat::completions::response::FinishReason,
    >,
    usage: Option<objectiveai::chat::completions::response::Usage>,
    service_tier: Option<String>,
) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: id.to_owned(),
        upstream_id: upstream_id.to_owned(),
        model: model.to_owned(),
        upstream_model: upstream_model.to_owned(),
        created,
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
