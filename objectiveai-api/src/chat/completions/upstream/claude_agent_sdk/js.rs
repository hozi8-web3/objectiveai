//! Inline JavaScript generation for the Claude Agent SDK subprocess.

/// Builds inline Node.js code that invokes the Claude Agent SDK `query()` function.
///
/// The generated script:
/// 1. Deletes `CLAUDECODE` env var to avoid conflicts
/// 2. Creates an async generator yielding the SDK user message
/// 3. Configures query options (model, system prompt, effort, thinking tokens, output format)
/// 4. Streams events to stdout as JSONL
pub fn build_js(
    system_prompt: &Option<String>,
    message_json: &str,
    model: &str,
    verbosity: Option<&str>,
    reasoning_max_tokens: Option<u64>,
    output_format_json: Option<&str>,
) -> String {
    let system_prompt_js = match system_prompt {
        Some(s) => {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('`', "\\`")
                .replace('$', "\\$");
            format!("opts.systemPrompt = `{escaped}`;")
        }
        None => String::new(),
    };

    let effort_js = match verbosity {
        Some(v) => format!("opts.effort = \"{v}\";"),
        None => String::new(),
    };

    let max_thinking_tokens_js = match reasoning_max_tokens {
        Some(t) => format!("opts.maxThinkingTokens = {t};"),
        None => String::new(),
    };

    let output_format_js = match output_format_json {
        Some(schema) => format!("opts.outputFormat = {schema};"),
        None => String::new(),
    };

    format!(
        r#"
delete process.env.CLAUDECODE;
const {{ query }} = require(process.env.CLAUDE_AGENT_SDK_PATH || "@anthropic-ai/claude-agent-sdk");

(async () => {{
  try {{
    const message = {message_json};

    async function* messages() {{
      yield message;
    }}

    const opts = {{
      permissionMode: "dontAsk",
      allowedTools: [],
      model: "{model}",
      includePartialMessages: true,
    }};
    {system_prompt_js}
    {effort_js}
    {max_thinking_tokens_js}
    {output_format_js}

    const stream = query({{ prompt: messages(), options: opts }});

    for await (const event of stream) {{
      process.stdout.write(JSON.stringify(event) + "\n");
    }}
  }} catch (e) {{
    process.stderr.write(e.message || String(e));
    process.exit(1);
  }}
}})();
"#
    )
}
