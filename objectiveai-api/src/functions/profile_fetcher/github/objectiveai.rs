//! ObjectiveAI API implementation of the GitHub Profile fetcher.

use crate::ctx;
use futures::FutureExt;
use objectiveai::error::StatusError;
use std::sync::Arc;

/// Fetches Profiles from GitHub via the ObjectiveAI API.
pub struct ObjectiveAiFetcher {
    /// The HTTP client for API requests.
    pub client: Arc<objectiveai::HttpClient>,
}

impl ObjectiveAiFetcher {
    /// Creates a new ObjectiveAI GitHub Profile fetcher.
    pub fn new(client: Arc<objectiveai::HttpClient>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl<CTXEXT> super::super::Fetcher<CTXEXT> for ObjectiveAiFetcher
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
        // Resolve commit (use latest_commit_cache if commit is None)
        let commit = if let Some(c) = commit {
            c.to_owned()
        } else {
            let profile_cache = ctx.profile_cache.clone();
            let shared = ctx
                .latest_commit_cache
                .entry((
                    objectiveai::functions::Remote::Github,
                    owner.to_owned(),
                    repository.to_owned(),
                ))
                .or_insert_with(|| {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let client = self.client.clone();
                    let owner = owner.to_owned();
                    let repository = repository.to_owned();
                    tokio::spawn(async move {
                        let result =
                            match objectiveai::functions::profiles::get_profile(
                                &client,
                                objectiveai::functions::Remote::Github,
                                &owner,
                                &repository,
                                None,
                            )
                            .await
                            {
                                Ok(profile) => {
                                    let commit = profile.commit.clone();
                                    // Populate profile_cache with the fetched result
                                    profile_cache
                                        .entry((
                                            objectiveai::functions::Remote::Github,
                                            profile.owner.clone(),
                                            profile.repository.clone(),
                                            commit.clone(),
                                        ))
                                        .or_insert_with(|| {
                                            let (tx, rx) =
                                                tokio::sync::oneshot::channel();
                                            let _ = tx.send(Ok(Some(profile.inner)));
                                            rx.shared()
                                        });
                                    Ok(Some(commit))
                                }
                                Err(e) if e.status() == 404 => Ok(None),
                                Err(e) => Err(
                                    objectiveai::error::ResponseError::from(&e),
                                ),
                            };
                        let _ = tx.send(result);
                    });
                    rx.shared()
                })
                .clone();
            match shared.await.unwrap() {
                Ok(Some(commit)) => commit,
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            }
        };

        // Fetch profile with resolved commit (cached)
        let shared = ctx
            .profile_cache
            .entry((
                objectiveai::functions::Remote::Github,
                owner.to_owned(),
                repository.to_owned(),
                commit.clone(),
            ))
            .or_insert_with(|| {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let client = self.client.clone();
                let owner = owner.to_owned();
                let repository = repository.to_owned();
                let commit = commit.clone();
                tokio::spawn(async move {
                    let result =
                        match objectiveai::functions::profiles::get_profile(
                            &client,
                            objectiveai::functions::Remote::Github,
                            &owner,
                            &repository,
                            Some(&commit),
                        )
                        .await
                        {
                            Ok(profile) => Ok(Some(profile.inner)),
                            Err(e) if e.status() == 404 => Ok(None),
                            Err(e) => {
                                Err(objectiveai::error::ResponseError::from(&e))
                            }
                        };
                    let _ = tx.send(result);
                });
                rx.shared()
            })
            .clone();
        match shared.await.unwrap() {
            Ok(Some(inner)) => {
                Ok(Some(objectiveai::functions::profiles::response::GetProfile {
                    remote: objectiveai::functions::Remote::Github,
                    owner: owner.to_owned(),
                    repository: repository.to_owned(),
                    commit,
                    inner,
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
