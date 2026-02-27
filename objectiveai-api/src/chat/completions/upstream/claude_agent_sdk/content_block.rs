//! Anthropic SDK content block types for message serialization.

use serde::{Deserialize, Serialize};

// --- Cache Control ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CacheControlEphemeralType {
    #[serde(rename = "ephemeral")]
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CacheTtl {
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheControlEphemeral {
    pub r#type: CacheControlEphemeralType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<CacheTtl>,
}

// --- Citations ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CitationsConfigParam {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TextCitationParamType {
    #[serde(rename = "char_location")]
    CharLocation,
    #[serde(rename = "page_location")]
    PageLocation,
    #[serde(rename = "content_block_location")]
    ContentBlockLocation,
    #[serde(rename = "web_search_result_location")]
    WebSearchResultLocation,
    #[serde(rename = "search_result_location")]
    SearchResultLocation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CitationCharLocationParam {
    pub r#type: TextCitationParamType,
    pub cited_text: String,
    pub document_index: u64,
    pub document_title: Option<String>,
    pub end_char_index: u64,
    pub start_char_index: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CitationPageLocationParam {
    pub r#type: TextCitationParamType,
    pub cited_text: String,
    pub document_index: u64,
    pub document_title: Option<String>,
    pub end_page_number: u64,
    pub start_page_number: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CitationContentBlockLocationParam {
    pub r#type: TextCitationParamType,
    pub cited_text: String,
    pub document_index: u64,
    pub document_title: Option<String>,
    pub end_block_index: u64,
    pub start_block_index: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CitationWebSearchResultLocationParam {
    pub r#type: TextCitationParamType,
    pub cited_text: String,
    pub encrypted_index: String,
    pub title: Option<String>,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CitationSearchResultLocationParam {
    pub r#type: TextCitationParamType,
    pub cited_text: String,
    pub end_block_index: u64,
    pub search_result_index: u64,
    pub source: String,
    pub start_block_index: u64,
    pub title: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TextCitationParam {
    CharLocation(CitationCharLocationParam),
    PageLocation(CitationPageLocationParam),
    ContentBlockLocation(CitationContentBlockLocationParam),
    WebSearchResultLocation(CitationWebSearchResultLocationParam),
    SearchResultLocation(CitationSearchResultLocationParam),
}

// --- Image Sources ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ImageMediaType {
    #[serde(rename = "image/jpeg")]
    Jpeg,
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/gif")]
    Gif,
    #[serde(rename = "image/webp")]
    Webp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Base64ImageSourceType {
    #[serde(rename = "base64")]
    Base64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Base64ImageSource {
    pub r#type: Base64ImageSourceType,
    pub data: String,
    pub media_type: ImageMediaType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum URLImageSourceType {
    #[serde(rename = "url")]
    Url,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct URLImageSource {
    pub r#type: URLImageSourceType,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ImageSource {
    Base64(Base64ImageSource),
    Url(URLImageSource),
}

// --- Document Sources ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PdfMediaType {
    #[serde(rename = "application/pdf")]
    Pdf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PlainTextMediaType {
    #[serde(rename = "text/plain")]
    Plain,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Base64PDFSourceType {
    #[serde(rename = "base64")]
    Base64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Base64PDFSource {
    pub r#type: Base64PDFSourceType,
    pub data: String,
    pub media_type: PdfMediaType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PlainTextSourceType {
    #[serde(rename = "text")]
    Text,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlainTextSource {
    pub r#type: PlainTextSourceType,
    pub data: String,
    pub media_type: PlainTextMediaType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum URLPDFSourceType {
    #[serde(rename = "url")]
    Url,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct URLPDFSource {
    pub r#type: URLPDFSourceType,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DocumentSource {
    Base64Pdf(Base64PDFSource),
    PlainText(PlainTextSource),
    Url(URLPDFSource),
}

// --- Block Param Structs ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TextBlockParamType {
    #[serde(rename = "text")]
    Text,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextBlockParam {
    pub r#type: TextBlockParamType,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citations: Option<Vec<TextCitationParam>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ImageBlockParamType {
    #[serde(rename = "image")]
    Image,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageBlockParam {
    pub r#type: ImageBlockParamType,
    pub source: ImageSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DocumentBlockParamType {
    #[serde(rename = "document")]
    Document,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentBlockParam {
    pub r#type: DocumentBlockParamType,
    pub source: DocumentSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControlEphemeral>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citations: Option<CitationsConfigParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

// --- Content Block Param (the main enum) ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ContentBlockParam {
    Text(TextBlockParam),
    Image(ImageBlockParam),
    Document(DocumentBlockParam),
}
