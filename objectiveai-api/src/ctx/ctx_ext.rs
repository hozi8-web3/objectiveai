//! Context extension trait for per-request customization.

/// Extension trait for providing per-request customization.
///
/// Implementations can provide BYOK (Bring Your Own Key) API keys
/// for upstream providers, allowing users to use their own API keys
/// instead of ObjectiveAI's pooled keys.
#[async_trait::async_trait]
pub trait ContextExt {
    /// Returns the user's BYOK API key for the given upstream provider.
    ///
    /// Returns `Ok(None)` if the user has not configured a BYOK key
    /// for this upstream, in which case the locally configured key will be used.
    async fn get_byok(
        &self,
        upstream: objectiveai::chat::completions::Upstream,
    ) -> Result<Option<String>, objectiveai::error::ResponseError>;
}
