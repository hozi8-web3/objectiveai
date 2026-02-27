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
    let mut opts_extra = Vec::new();
    if let Some(s) = system_prompt {
        let escaped = s
            .replace('\\', "\\\\")
            .replace('`', "\\`")
            .replace('$', "\\$");
        opts_extra.push(format!("    opts.systemPrompt = `{escaped}`;"));
    }
    if let Some(v) = verbosity {
        opts_extra.push(format!("    opts.effort = \"{v}\";"));
    }
    if let Some(t) = reasoning_max_tokens {
        opts_extra.push(format!("    opts.maxThinkingTokens = {t};"));
    }
    if let Some(schema) = output_format_json {
        opts_extra.push(format!("    opts.outputFormat = {schema};"));
    }
    let opts_extra_js = if opts_extra.is_empty() {
        String::new()
    } else {
        format!("\n{}", opts_extra.join("\n"))
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
    }};{opts_extra_js}

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
