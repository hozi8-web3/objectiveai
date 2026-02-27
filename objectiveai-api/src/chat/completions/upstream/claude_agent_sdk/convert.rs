//! Converts ObjectiveAI message types to Claude Agent SDK format.

use objectiveai::chat::completions::request::{
    DeveloperMessage, File, ImageUrl, Message, RichContent, RichContentPart,
    SimpleContent, SimpleContentPart, SystemMessage, UserMessage,
};

use super::content_block::{
    Base64ImageSource, Base64ImageSourceType, Base64PDFSource,
    Base64PDFSourceType, ContentBlockParam, DocumentBlockParam,
    DocumentBlockParamType, DocumentSource, ImageBlockParam,
    ImageBlockParamType, ImageMediaType, ImageSource, PdfMediaType,
    PlainTextMediaType, PlainTextSource, PlainTextSourceType, TextBlockParam,
    TextBlockParamType, URLImageSource, URLImageSourceType, URLPDFSource,
    URLPDFSourceType,
};
use super::sdk_message::{
    MessageContent, MessageParam, MessageRole, SDKUserMessage,
    SDKUserMessageType,
};

// --- Helpers ---

fn to_image_media_type(raw: &str) -> Result<ImageMediaType, String> {
    match raw {
        "image/jpeg" | "image/jpg" => Ok(ImageMediaType::Jpeg),
        "image/png" => Ok(ImageMediaType::Png),
        "image/gif" => Ok(ImageMediaType::Gif),
        "image/webp" => Ok(ImageMediaType::Webp),
        _ => Err(format!("unsupported image media type: {raw}")),
    }
}

fn make_text_block(text: String) -> ContentBlockParam {
    ContentBlockParam::Text(TextBlockParam {
        r#type: TextBlockParamType::Text,
        text,
        cache_control: None,
        citations: None,
    })
}

fn make_document_block(
    source: DocumentSource,
    title: Option<String>,
) -> ContentBlockParam {
    ContentBlockParam::Document(DocumentBlockParam {
        r#type: DocumentBlockParamType::Document,
        source,
        cache_control: None,
        citations: None,
        context: None,
        title,
    })
}

/// Convert a single ObjectiveAI RichContentPart to an Anthropic ContentBlockParam.
fn rich_part_to_content_block(
    part: &RichContentPart,
) -> Result<ContentBlockParam, String> {
    match part {
        RichContentPart::Text { text } => Ok(make_text_block(text.clone())),

        RichContentPart::ImageUrl { image_url } => {
            image_url_to_block(image_url)
        }

        RichContentPart::InputAudio { input_audio } => Err(format!(
            "unsupported content type: audio ({} format, {} base64 chars)",
            input_audio.format,
            input_audio.data.len()
        )),

        RichContentPart::VideoUrl { video_url }
        | RichContentPart::InputVideo { video_url } => Err(format!(
            "unsupported content type: video ({})",
            video_url.url
        )),

        RichContentPart::File { file } => file_to_block(file),
    }
}

fn image_url_to_block(
    image_url: &ImageUrl,
) -> Result<ContentBlockParam, String> {
    let url = &image_url.url;

    if url.starts_with("data:") {
        // Parse data URI: data:<media_type>;base64,<data>
        let comma_index = match url.find(',') {
            Some(i) => i,
            None => {
                return Err("unsupported image: invalid data URI (no comma)"
                    .to_string());
            }
        };
        let meta = &url[5..comma_index]; // after "data:" before ","
        let raw_media_type = meta.split(';').next().unwrap_or("");
        let data = &url[comma_index + 1..];
        let media_type = to_image_media_type(raw_media_type)?;

        Ok(ContentBlockParam::Image(ImageBlockParam {
            r#type: ImageBlockParamType::Image,
            source: ImageSource::Base64(Base64ImageSource {
                r#type: Base64ImageSourceType::Base64,
                data: data.to_owned(),
                media_type,
            }),
            cache_control: None,
        }))
    } else {
        Ok(ContentBlockParam::Image(ImageBlockParam {
            r#type: ImageBlockParamType::Image,
            source: ImageSource::Url(URLImageSource {
                r#type: URLImageSourceType::Url,
                url: url.clone(),
            }),
            cache_control: None,
        }))
    }
}

fn file_to_block(file: &File) -> Result<ContentBlockParam, String> {
    // URL-based file: treat as URL PDF source
    if let Some(ref file_url) = file.file_url {
        let source = DocumentSource::Url(URLPDFSource {
            r#type: URLPDFSourceType::Url,
            url: file_url.clone(),
        });
        return Ok(make_document_block(source, file.filename.clone()));
    }

    // Base64 file data
    if let Some(ref file_data) = file.file_data {
        let is_pdf = file
            .filename
            .as_ref()
            .map(|n| n.to_lowercase().ends_with(".pdf"))
            .unwrap_or(false);

        if is_pdf {
            let source = DocumentSource::Base64Pdf(Base64PDFSource {
                r#type: Base64PDFSourceType::Base64,
                data: file_data.clone(),
                media_type: PdfMediaType::Pdf,
            });
            return Ok(make_document_block(source, file.filename.clone()));
        }

        // Non-PDF: pass as plain text
        let source = DocumentSource::PlainText(PlainTextSource {
            r#type: PlainTextSourceType::Text,
            data: file_data.clone(),
            media_type: PlainTextMediaType::Plain,
        });
        return Ok(make_document_block(source, file.filename.clone()));
    }

    // No data or URL
    let desc = file
        .filename
        .as_deref()
        .or(file.file_id.as_deref())
        .unwrap_or("unknown");
    Err(format!(
        "unsupported file: no data or URL provided ({desc})"
    ))
}

// --- Content conversion ---

/// Convert RichContent (string or parts) to ContentBlockParam vec.
fn rich_content_to_blocks(
    content: &RichContent,
) -> Result<Vec<ContentBlockParam>, String> {
    match content {
        RichContent::Text(s) => {
            if s.is_empty() {
                Ok(vec![])
            } else {
                Ok(vec![make_text_block(s.clone())])
            }
        }
        RichContent::Parts(parts) => {
            parts.iter().map(rich_part_to_content_block).collect()
        }
    }
}

/// Convert SimpleContent (string or text parts) to ContentBlockParam vec.
fn simple_content_to_blocks(content: &SimpleContent) -> Vec<ContentBlockParam> {
    match content {
        SimpleContent::Text(s) => {
            if s.is_empty() {
                vec![]
            } else {
                vec![make_text_block(s.clone())]
            }
        }
        SimpleContent::Parts(parts) => parts
            .iter()
            .map(|p| match p {
                SimpleContentPart::Text { text } => {
                    make_text_block(text.clone())
                }
            })
            .collect(),
    }
}

/// Extract plain text from SimpleContent.
fn simple_content_to_text(content: &SimpleContent) -> String {
    match content {
        SimpleContent::Text(s) => s.clone(),
        SimpleContent::Parts(parts) => parts
            .iter()
            .map(|p| match p {
                SimpleContentPart::Text { text } => text.as_str(),
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
    }
}

/// Convert a non-leading message's content to blocks.
fn message_to_blocks(msg: &Message) -> Result<Vec<ContentBlockParam>, String> {
    match msg {
        Message::System(SystemMessage { content, .. })
        | Message::Developer(DeveloperMessage { content, .. }) => {
            Ok(simple_content_to_blocks(content))
        }
        Message::User(UserMessage { content, .. }) => {
            rich_content_to_blocks(content)
        }
        Message::Assistant(_) => {
            Err("unsupported: assistant messages cannot be converted".into())
        }
        Message::Tool(_) => {
            Err("unsupported: tool messages cannot be converted".into())
        }
    }
}

// --- Main convert function ---

/// Converts ObjectiveAI messages into a system prompt and SDK user message.
///
/// Leading system/developer messages are extracted as the system prompt.
/// Remaining messages are flattened into a single SDK user message with content blocks.
pub fn convert(
    messages: &[Message],
) -> Result<(Option<String>, SDKUserMessage), String> {
    if messages.is_empty() {
        return Err("no messages to convert".into());
    }

    // Phase 1: Extract system prompt from leading system/developer messages
    let mut system_parts: Vec<String> = Vec::new();
    let mut start = 0;
    for msg in messages {
        match msg {
            Message::System(SystemMessage { content, .. })
            | Message::Developer(DeveloperMessage { content, .. }) => {
                let text = simple_content_to_text(content);
                if !text.is_empty() {
                    system_parts.push(text);
                }
                start += 1;
            }
            _ => break,
        }
    }
    let system_prompt = if system_parts.is_empty() {
        None
    } else {
        Some(system_parts.join("\n\n"))
    };
    let rest = &messages[start..];

    if rest.is_empty() {
        return Err("no non-system messages to convert".into());
    }

    // Phase 2: Convert all remaining messages into one flat content block list.
    let mut all_blocks: Vec<ContentBlockParam> = Vec::new();
    for (i, msg) in rest.iter().enumerate() {
        let blocks = message_to_blocks(msg)?;
        if i > 0 && !blocks.is_empty() {
            all_blocks.push(make_text_block("\n\n".into()));
        }
        all_blocks.extend(blocks);
    }

    // Phase 3: Wrap in a single SDKUserMessage
    let session_id = uuid::Uuid::new_v4().to_string();
    let sdk_message = SDKUserMessage {
        r#type: SDKUserMessageType::User,
        message: MessageParam {
            role: MessageRole::User,
            content: MessageContent::Blocks(all_blocks),
        },
        parent_tool_use_id: None,
        is_synthetic: None,
        tool_use_result: None,
        uuid: None,
        session_id,
    };

    Ok((system_prompt, sdk_message))
}
