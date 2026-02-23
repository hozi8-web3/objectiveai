//! GitHub API implementation of the Profile fetcher.

use crate::{ctx, functions};
use std::sync::Arc;

/// Fetches Profiles from GitHub directly via the GitHub API.
pub struct GithubFetcher {
    pub client: Arc<functions::github::Client>,
}

impl GithubFetcher {
    pub fn new(client: Arc<functions::github::Client>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl<CTXEXT> super::super::Fetcher<CTXEXT> for GithubFetcher
where
    CTXEXT: Send + Sync + 'static,
{
    async fn fetch(
        &self,
        ctx: ctx::Context<CTXEXT>,
        owner: &str,
        repository: &str,
        commit: Option<&str>,
    ) -> Result<
        Option<objectiveai::functions::profiles::response::GetProfile>,
        objectiveai::error::ResponseError,
    > {
        self.client
            .clone()
            .fetch_profile(ctx, owner, repository, commit)
            .await
    }
}
