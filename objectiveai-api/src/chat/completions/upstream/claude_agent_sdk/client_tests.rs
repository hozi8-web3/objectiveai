//! Tests for Claude Agent SDK JavaScript compilation.

use super::client::compile_js_for_vector;
use objectiveai::chat::completions::request::{
    AssistantMessage, DeveloperMessage, File, ImageUrl, Message, RichContent,
    RichContentPart, SimpleContent, SystemMessage, UserMessage,
};
use objectiveai::ensemble::EnsembleBase;
use objectiveai::ensemble_llm::{
    EnsembleLlm, EnsembleLlmBase, OutputMode, WithFallbacksAndCount,
};

/// Helper to build a VectorCompletionCreateParams.
fn vector_request(
    messages: Vec<Message>,
    responses: Vec<&str>,
    ensemble: EnsembleBase,
) -> objectiveai::vector::completions::request::VectorCompletionCreateParams {
    objectiveai::vector::completions::request::VectorCompletionCreateParams {
        messages,
        responses: responses
            .into_iter()
            .map(|s| RichContent::Text(s.to_owned()))
            .collect(),
        ensemble: objectiveai::vector::completions::request::Ensemble::Provided(
            ensemble,
        ),
        profile: objectiveai::vector::completions::request::Profile::Weights(
            vec![rust_decimal::Decimal::ONE],
        ),
        retry: None,
        from_cache: None,
        from_rng: None,
        upstreams: None,
        provider: None,
        seed: None,
        stream: None,
        tools: None,
        backoff_max_elapsed_time: None,
        first_chunk_timeout: None,
        other_chunk_timeout: None,
    }
}

/// Replaces the random session_id UUID with a fixed placeholder.
fn normalize_js(js: &str) -> String {
    let re = regex::Regex::new(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    )
    .unwrap();
    re.replace_all(js, "SESSION_ID").to_string()
}

#[test]
fn instruction_system_prompt_with_responses() {
    let js = normalize_js(
        &compile_js_for_vector(
            &EnsembleLlm {
                id: "test".into(),
                base: EnsembleLlmBase {
                    model: "anthropic/claude-haiku-4.5".into(),
                    output_mode: OutputMode::Instruction,
                    ..Default::default()
                },
            },
            &vector_request(
                vec![
                    Message::System(SystemMessage {
                        content: SimpleContent::Text(
                            "you are a mathematician".into(),
                        ),
                        name: None,
                    }),
                    Message::User(UserMessage {
                        content: RichContent::Text("what is 2+2".into()),
                        name: None,
                    }),
                ],
                vec!["4", "22"],
                EnsembleBase {
                    llms: vec![WithFallbacksAndCount {
                        count: 1,
                        inner: EnsembleLlmBase {
                            model: "anthropic/claude-haiku-4.5".into(),
                            output_mode: OutputMode::Instruction,
                            ..Default::default()
                        },
                        fallbacks: None,
                    }],
                },
            ),
            &[("`F`".to_string(), 0), ("`P`".to_string(), 1)],
        )
        .unwrap(),
    );

    assert_eq!(js, r#"
delete process.env.CLAUDECODE;
const { query } = require(process.env.CLAUDE_AGENT_SDK_PATH || "@anthropic-ai/claude-agent-sdk");

(async () => {
  try {
    const message = {"type":"user","message":{"content":[{"type":"text","text":"what is 2+2"},{"type":"text","text":"\n\nSelect the response:\n\n"},{"type":"text","text":"{\n    \"`F`\": \""},{"type":"text","text":"4"},{"type":"text","text":"\",\n    \"`P`\": \""},{"type":"text","text":"22"},{"type":"text","text":"\"\n}"},{"type":"text","text":"\n\n"},{"type":"text","text":"Output one response key including backticks\n- `F`\n- `P`"}],"role":"user"},"parent_tool_use_id":null,"session_id":"SESSION_ID"};

    async function* messages() {
      yield message;
    }

    const opts = {
      permissionMode: "dontAsk",
      allowedTools: [],
      model: "claude-haiku-4-5",
      includePartialMessages: true,
    };
    opts.systemPrompt = `you are a mathematician`;

    const stream = query({ prompt: messages(), options: opts });

    for await (const event of stream) {
      process.stdout.write(JSON.stringify(event) + "\n");
    }
  } catch (e) {
    process.stderr.write(e.message || String(e));
    process.exit(1);
  }
})();
"#);
}

#[test]
fn instruction_system_and_developer_prompt_with_responses() {
    let js = normalize_js(
        &compile_js_for_vector(
            &EnsembleLlm {
                id: "test".into(),
                base: EnsembleLlmBase {
                    model: "anthropic/claude-haiku-4.5".into(),
                    output_mode: OutputMode::Instruction,
                    ..Default::default()
                },
            },
            &vector_request(
                vec![
                    Message::System(SystemMessage {
                        content: SimpleContent::Text(
                            "you are a mathematician".into(),
                        ),
                        name: None,
                    }),
                    Message::Developer(DeveloperMessage {
                        content: SimpleContent::Text(
                            "who is very good at math".into(),
                        ),
                        name: None,
                    }),
                    Message::User(UserMessage {
                        content: RichContent::Text("what is 2+2".into()),
                        name: None,
                    }),
                ],
                vec!["4", "22"],
                EnsembleBase {
                    llms: vec![WithFallbacksAndCount {
                        count: 1,
                        inner: EnsembleLlmBase {
                            model: "anthropic/claude-haiku-4.5".into(),
                            output_mode: OutputMode::Instruction,
                            ..Default::default()
                        },
                        fallbacks: None,
                    }],
                },
            ),
            &[("`F`".to_string(), 0), ("`P`".to_string(), 1)],
        )
        .unwrap(),
    );

    assert_eq!(js, r#"
delete process.env.CLAUDECODE;
const { query } = require(process.env.CLAUDE_AGENT_SDK_PATH || "@anthropic-ai/claude-agent-sdk");

(async () => {
  try {
    const message = {"type":"user","message":{"content":[{"type":"text","text":"what is 2+2"},{"type":"text","text":"\n\nSelect the response:\n\n"},{"type":"text","text":"{\n    \"`F`\": \""},{"type":"text","text":"4"},{"type":"text","text":"\",\n    \"`P`\": \""},{"type":"text","text":"22"},{"type":"text","text":"\"\n}"},{"type":"text","text":"\n\n"},{"type":"text","text":"Output one response key including backticks\n- `F`\n- `P`"}],"role":"user"},"parent_tool_use_id":null,"session_id":"SESSION_ID"};

    async function* messages() {
      yield message;
    }

    const opts = {
      permissionMode: "dontAsk",
      allowedTools: [],
      model: "claude-haiku-4-5",
      includePartialMessages: true,
    };
    opts.systemPrompt = `you are a mathematician

who is very good at math`;

    const stream = query({ prompt: messages(), options: opts });

    for await (const event of stream) {
      process.stdout.write(JSON.stringify(event) + "\n");
    }
  } catch (e) {
    process.stderr.write(e.message || String(e));
    process.exit(1);
  }
})();
"#);
}

#[test]
fn assistant_prefix_message_errors() {
    let err = compile_js_for_vector(
        &EnsembleLlm {
            id: "test".into(),
            base: EnsembleLlmBase {
                model: "anthropic/claude-haiku-4.5".into(),
                output_mode: OutputMode::Instruction,
                prefix_messages: Some(vec![
                    Message::User(UserMessage {
                        content: RichContent::Text("hello".into()),
                        name: None,
                    }),
                    Message::Assistant(AssistantMessage {
                        content: Some(RichContent::Text("hi there".into())),
                        name: None,
                        refusal: None,
                        tool_calls: None,
                        reasoning: None,
                    }),
                ]),
                ..Default::default()
            },
        },
        &vector_request(
            vec![Message::User(UserMessage {
                content: RichContent::Text("what is 2+2".into()),
                name: None,
            })],
            vec!["4", "22"],
            EnsembleBase {
                llms: vec![WithFallbacksAndCount {
                    count: 1,
                    inner: EnsembleLlmBase {
                        model: "anthropic/claude-haiku-4.5".into(),
                        output_mode: OutputMode::Instruction,
                        ..Default::default()
                    },
                    fallbacks: None,
                }],
            },
        ),
        &[("`F`".to_string(), 0), ("`P`".to_string(), 1)],
    );

    assert!(err.is_err());
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("assistant"),
        "error should mention assistant messages: {msg}"
    );
}

#[test]
fn developer_prefix_and_system_suffix_with_responses() {
    let js = normalize_js(
        &compile_js_for_vector(
            &EnsembleLlm {
                id: "test".into(),
                base: EnsembleLlmBase {
                    model: "anthropic/claude-haiku-4.5".into(),
                    output_mode: OutputMode::Instruction,
                    prefix_messages: Some(vec![Message::Developer(
                        DeveloperMessage {
                            content: SimpleContent::Text(
                                "be concise".into(),
                            ),
                            name: None,
                        },
                    )]),
                    suffix_messages: Some(vec![Message::System(
                        SystemMessage {
                            content: SimpleContent::Text(
                                "remember to be brief".into(),
                            ),
                            name: None,
                        },
                    )]),
                    ..Default::default()
                },
            },
            &vector_request(
                vec![
                    Message::System(SystemMessage {
                        content: SimpleContent::Text(
                            "you are a mathematician".into(),
                        ),
                        name: None,
                    }),
                    Message::User(UserMessage {
                        content: RichContent::Text("what is 2+2".into()),
                        name: None,
                    }),
                ],
                vec!["4", "22"],
                EnsembleBase {
                    llms: vec![WithFallbacksAndCount {
                        count: 1,
                        inner: EnsembleLlmBase {
                            model: "anthropic/claude-haiku-4.5".into(),
                            output_mode: OutputMode::Instruction,
                            ..Default::default()
                        },
                        fallbacks: None,
                    }],
                },
            ),
            &[("`F`".to_string(), 0), ("`P`".to_string(), 1)],
        )
        .unwrap(),
    );

    assert_eq!(js, r#"
delete process.env.CLAUDECODE;
const { query } = require(process.env.CLAUDE_AGENT_SDK_PATH || "@anthropic-ai/claude-agent-sdk");

(async () => {
  try {
    const message = {"type":"user","message":{"content":[{"type":"text","text":"what is 2+2"},{"type":"text","text":"\n\nSelect the response:\n\n"},{"type":"text","text":"{\n    \"`F`\": \""},{"type":"text","text":"4"},{"type":"text","text":"\",\n    \"`P`\": \""},{"type":"text","text":"22"},{"type":"text","text":"\"\n}"},{"type":"text","text":"\n\n"},{"type":"text","text":"remember to be brief"},{"type":"text","text":"\n\nOutput one response key including backticks:\n- `F`\n- `P`"}],"role":"user"},"parent_tool_use_id":null,"session_id":"SESSION_ID"};

    async function* messages() {
      yield message;
    }

    const opts = {
      permissionMode: "dontAsk",
      allowedTools: [],
      model: "claude-haiku-4-5",
      includePartialMessages: true,
    };
    opts.systemPrompt = `be concise

you are a mathematician`;

    const stream = query({ prompt: messages(), options: opts });

    for await (const event of stream) {
      process.stdout.write(JSON.stringify(event) + "\n");
    }
  } catch (e) {
    process.stderr.write(e.message || String(e));
    process.exit(1);
  }
})();
"#);
}

#[test]
fn rich_content_parts_with_files_and_images() {
    let js = normalize_js(
        &compile_js_for_vector(
            &EnsembleLlm {
                id: "test".into(),
                base: EnsembleLlmBase {
                    model: "anthropic/claude-haiku-4.5".into(),
                    output_mode: OutputMode::Instruction,
                    ..Default::default()
                },
            },
            &vector_request(
                vec![Message::User(UserMessage {
                    content: RichContent::Parts(vec![
                        RichContentPart::File {
                            file: File {
                                file_data: Some("JVBER==".into()),
                                file_id: None,
                                filename: Some("doc.pdf".into()),
                                file_url: None,
                            },
                        },
                        RichContentPart::File {
                            file: File {
                                file_data: Some("SGVsbG8=".into()),
                                file_id: None,
                                filename: Some("notes.txt".into()),
                                file_url: None,
                            },
                        },
                        RichContentPart::ImageUrl {
                            image_url: ImageUrl {
                                url: "data:image/png;base64,iVBOR==".into(),
                                detail: None,
                            },
                        },
                        RichContentPart::ImageUrl {
                            image_url: ImageUrl {
                                url: "https://example.com/photo.jpg".into(),
                                detail: None,
                            },
                        },
                        RichContentPart::File {
                            file: File {
                                file_data: None,
                                file_id: None,
                                filename: None,
                                file_url: Some(
                                    "https://example.com/report.pdf".into(),
                                ),
                            },
                        },
                        RichContentPart::Text {
                            text: "describe these".into(),
                        },
                    ]),
                    name: None,
                })],
                vec!["good", "bad"],
                EnsembleBase {
                    llms: vec![WithFallbacksAndCount {
                        count: 1,
                        inner: EnsembleLlmBase {
                            model: "anthropic/claude-haiku-4.5".into(),
                            output_mode: OutputMode::Instruction,
                            ..Default::default()
                        },
                        fallbacks: None,
                    }],
                },
            ),
            &[("`F`".to_string(), 0), ("`P`".to_string(), 1)],
        )
        .unwrap(),
    );

    assert_eq!(js, r#"
delete process.env.CLAUDECODE;
const { query } = require(process.env.CLAUDE_AGENT_SDK_PATH || "@anthropic-ai/claude-agent-sdk");

(async () => {
  try {
    const message = {"type":"user","message":{"content":[{"type":"document","source":{"type":"base64","data":"JVBER==","media_type":"application/pdf"},"title":"doc.pdf"},{"type":"document","source":{"type":"text","data":"SGVsbG8=","media_type":"text/plain"},"title":"notes.txt"},{"type":"image","source":{"type":"base64","data":"iVBOR==","media_type":"image/png"}},{"type":"image","source":{"type":"url","url":"https://example.com/photo.jpg"}},{"type":"document","source":{"type":"url","url":"https://example.com/report.pdf"}},{"type":"text","text":"describe these"},{"type":"text","text":"\n\nSelect the response:\n\n"},{"type":"text","text":"{\n    \"`F`\": \""},{"type":"text","text":"good"},{"type":"text","text":"\",\n    \"`P`\": \""},{"type":"text","text":"bad"},{"type":"text","text":"\"\n}"},{"type":"text","text":"\n\n"},{"type":"text","text":"Output one response key including backticks\n- `F`\n- `P`"}],"role":"user"},"parent_tool_use_id":null,"session_id":"SESSION_ID"};

    async function* messages() {
      yield message;
    }

    const opts = {
      permissionMode: "dontAsk",
      allowedTools: [],
      model: "claude-haiku-4-5",
      includePartialMessages: true,
    };

    const stream = query({ prompt: messages(), options: opts });

    for await (const event of stream) {
      process.stdout.write(JSON.stringify(event) + "\n");
    }
  } catch (e) {
    process.stderr.write(e.message || String(e));
    process.exit(1);
  }
})();
"#);
}
