/// Default context extension
#[derive(Clone)]
pub struct DefaultContextExt;

#[async_trait::async_trait]
impl super::ContextExt for DefaultContextExt {
    async fn get_byok(
        &self,
        _upstream: objectiveai::chat::completions::Upstream,
    ) -> Result<Option<String>, objectiveai::error::ResponseError> {
        Ok(None)
    }
}
