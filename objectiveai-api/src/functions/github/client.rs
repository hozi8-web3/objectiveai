use crate::ctx;
use futures::FutureExt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Client {
    pub http_client: reqwest::Client,
    pub github_token: Option<String>,
    pub user_agent: Option<String>,
    pub x_title: Option<String>,
    pub referer: Option<String>,
    pub backoff: backoff::ExponentialBackoff,
}

impl Client {
    pub fn new(
        http_client: reqwest::Client,
        github_token: Option<String>,
        user_agent: Option<String>,
        x_title: Option<String>,
        referer: Option<String>,
        backoff: backoff::ExponentialBackoff,
    ) -> Self {
        Self {
            http_client,
            github_token,
            user_agent,
            x_title,
            referer,
            backoff,
        }
    }

    pub async fn fetch_function<CTXEXT>(
        self: Arc<Self>,
        ctx: ctx::Context<CTXEXT>,
        owner: &str,
        repository: &str,
        commit: Option<&str>,
    ) -> Result<
        Option<objectiveai::functions::response::GetFunction>,
        objectiveai::error::ResponseError,
    >
    where
        CTXEXT: Send + Sync + 'static,
    {
        let commit = if let Some(c) = commit {
            c.to_owned()
        } else {
            match self
                .clone()
                .fetch_latest_commit(ctx.clone(), owner, repository)
                .await?
            {
                Some(sha) => sha,
                None => return Ok(None),
            }
        };
        let shared = ctx
            .function_cache
            .entry((objectiveai::functions::Remote::Github, owner.to_owned(), repository.to_owned(), commit.clone()))
            .or_insert_with(|| {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let client = self.clone();
                let owner = owner.to_owned();
                let repository = repository.to_owned();
                let commit = commit.clone();
                tokio::spawn(async move {
                    let result = client
                        .fetch_function_uncached(&owner, &repository, &commit)
                        .await
                        .map_err(|e| {
                            objectiveai::error::ResponseError::from(&e)
                        });
                    let _ = tx.send(result);
                });
                rx.shared()
            })
            .clone();
        match shared.await.unwrap() {
            Ok(Some(inner)) => {
                Ok(Some(objectiveai::functions::response::GetFunction {
                    remote: objectiveai::functions::Remote::Github,
                    owner: owner.to_owned(),
                    repository: repository.to_owned(),
                    commit: commit.to_owned(),
                    inner,
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn fetch_function_uncached(
        &self,
        owner: &str,
        repository: &str,
        commit: &str,
    ) -> Result<Option<objectiveai::functions::RemoteFunction>, super::Error>
    {
        self.fetch_file::<objectiveai::functions::RemoteFunction>(
            owner,
            repository,
            commit,
            "function.json",
        )
        .await
    }

    pub async fn fetch_profile<CTXEXT>(
        self: Arc<Self>,
        ctx: ctx::Context<CTXEXT>,
        owner: &str,
        repository: &str,
        commit: Option<&str>,
    ) -> Result<
        Option<objectiveai::functions::profiles::response::GetProfile>,
        objectiveai::error::ResponseError,
    >
    where
        CTXEXT: Send + Sync + 'static,
    {
        let commit = if let Some(c) = commit {
            c.to_owned()
        } else {
            match self
                .clone()
                .fetch_latest_commit(ctx.clone(), owner, repository)
                .await?
            {
                Some(sha) => sha,
                None => return Ok(None),
            }
        };
        let shared = ctx
            .profile_cache
            .entry((objectiveai::functions::Remote::Github, owner.to_owned(), repository.to_owned(), commit.clone()))
            .or_insert_with(|| {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let client = self.clone();
                let owner = owner.to_owned();
                let repository = repository.to_owned();
                let commit = commit.clone();
                tokio::spawn(async move {
                    let result = client
                        .fetch_profile_uncached(&owner, &repository, &commit)
                        .await
                        .map_err(|e| {
                            objectiveai::error::ResponseError::from(&e)
                        });
                    let _ = tx.send(result);
                });
                rx.shared()
            })
            .clone();
        match shared.await.unwrap() {
            Ok(Some(inner)) => Ok(Some(
                objectiveai::functions::profiles::response::GetProfile {
                    remote: objectiveai::functions::Remote::Github,
                    owner: owner.to_owned(),
                    repository: repository.to_owned(),
                    commit: commit.to_owned(),
                    inner,
                },
            )),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn fetch_profile_uncached(
        &self,
        owner: &str,
        repository: &str,
        commit: &str,
    ) -> Result<Option<objectiveai::functions::RemoteProfile>, super::Error>
    {
        match self
            .fetch_file::<objectiveai::functions::RemoteProfile>(
                owner,
                repository,
                commit,
                "profile.json",
            )
            .await
        {
            Ok(Some(profile)) => {
                let valid = match &profile {
                    objectiveai::functions::RemoteProfile::Tasks(tasks_profile) => {
                        tasks_profile.tasks.iter().all(|t| t.validate_commit_required())
                    }
                    objectiveai::functions::RemoteProfile::Auto(_) => true,
                };
                if !valid {
                    Err(super::Error::ProfileCommitShaRequired)
                } else {
                    Ok(Some(profile))
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn fetch_latest_commit<CTXEXT>(
        self: Arc<Self>,
        ctx: ctx::Context<CTXEXT>,
        owner: &str,
        repository: &str,
    ) -> Result<Option<String>, objectiveai::error::ResponseError>
    where
        CTXEXT: Send + Sync + 'static,
    {
        let shared = ctx
            .latest_commit_cache
            .entry((objectiveai::functions::Remote::Github, owner.to_owned(), repository.to_owned()))
            .or_insert_with(|| {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let owner = owner.to_owned();
                let repository = repository.to_owned();
                tokio::spawn(async move {
                    let result = self
                        .fetch_latest_commit_uncached(&owner, &repository)
                        .await
                        .map_err(|e| {
                            objectiveai::error::ResponseError::from(&e)
                        });
                    let _ = tx.send(result);
                });
                rx.shared()
            })
            .clone();
        shared.await.unwrap()
    }

    async fn fetch_latest_commit_uncached(
        &self,
        owner: &str,
        repository: &str,
    ) -> Result<Option<String>, super::Error> {
        #[derive(serde::Deserialize)]
        struct Commit {
            sha: String,
        }
        let http_request = self.request_headers(
            self.http_client
                .get(format!(
                    "https://api.github.com/repos/{}/{}/commits",
                    owner, repository,
                ))
                .header("accept", "application/vnd.github+json"),
        );
        backoff::future::retry(self.backoff.clone(), || async {
            let response = http_request
                .try_clone()
                .unwrap()
                .send()
                .await
                .map_err(super::Error::RequestError)?;
            let code = response.status();
            if code.is_success() {
                let text = response
                    .text()
                    .await
                    .map_err(super::Error::ResponseError)?;
                let mut de = serde_json::Deserializer::from_str(&text);
                match serde_path_to_error::deserialize::<_, Vec<Commit>>(
                    &mut de,
                ) {
                    Ok(commits) => Ok(commits.first().map(|c| c.sha.clone())),
                    Err(e) => Err(backoff::Error::transient(
                        super::Error::DeserializationError(e),
                    )),
                }
            } else if code == reqwest::StatusCode::NOT_FOUND {
                Ok(None)
            } else {
                match response.text().await {
                    Ok(text) => Err(backoff::Error::transient(
                        super::Error::BadStatus {
                            code,
                            body: match serde_json::from_str::<
                                serde_json::Value,
                            >(&text) {
                                Ok(json) => json,
                                Err(_) => serde_json::Value::String(text),
                            },
                        },
                    )),
                    Err(_) => Err(backoff::Error::transient(
                        super::Error::BadStatus {
                            code,
                            body: serde_json::Value::Null,
                        },
                    )),
                }
            }
        })
        .await
    }

    async fn fetch_file<T>(
        &self,
        owner: &str,
        repository: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<T>, super::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        backoff::future::retry(self.backoff.clone(), || async {
            match self.fetch_file_raw(owner, repository, commit, path).await {
                Ok(opt) => Ok(opt),
                Err(e1) => match self
                    .fetch_file_api(owner, repository, commit, path)
                    .await
                {
                    Ok(opt) => Ok(opt),
                    Err(e2) => Err(backoff::Error::transient(
                        super::Error::MultipleErrors(
                            Box::new(e1),
                            Box::new(e2),
                        ),
                    )),
                },
            }
        })
        .await
    }

    async fn fetch_file_raw<T>(
        &self,
        owner: &str,
        repository: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<T>, super::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let http_request = self.request_headers(self.http_client.get(format!(
            "https://raw.githubusercontent.com/{}/{}/{}/{}",
            owner, repository, commit, path,
        )));
        let response = http_request
            .send()
            .await
            .map_err(super::Error::RequestError)?;
        let code = response.status();
        if code.is_success() {
            let text =
                response.text().await.map_err(super::Error::ResponseError)?;
            let mut de = serde_json::Deserializer::from_str(&text);
            match serde_path_to_error::deserialize::<_, T>(&mut de) {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(super::Error::DeserializationError(e)),
            }
        } else if code == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            match response.text().await {
                Ok(text) => Err(super::Error::BadStatus {
                    code,
                    body: match serde_json::from_str::<serde_json::Value>(&text)
                    {
                        Ok(json) => json,
                        Err(_) => serde_json::Value::String(text),
                    },
                }),
                Err(_) => Err(super::Error::BadStatus {
                    code,
                    body: serde_json::Value::Null,
                }),
            }
        }
    }

    async fn fetch_file_api<T>(
        &self,
        owner: &str,
        repository: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<T>, super::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let http_request = self.request_headers(
            self.http_client
                .get(format!(
                    "https://api.github.com/repos/{}/{}/contents/{}?ref={}",
                    owner, repository, path, commit,
                ))
                .header("accept", "application/vnd.github+json"),
        );
        let response = http_request
            .send()
            .await
            .map_err(super::Error::RequestError)?;
        let code = response.status();
        if code.is_success() {
            let text =
                response.text().await.map_err(super::Error::ResponseError)?;
            let mut de = serde_json::Deserializer::from_str(&text);
            match serde_path_to_error::deserialize::<_, T>(&mut de) {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(super::Error::DeserializationError(e)),
            }
        } else if code == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            match response.text().await {
                Ok(text) => Err(super::Error::BadStatus {
                    code,
                    body: match serde_json::from_str::<serde_json::Value>(&text)
                    {
                        Ok(json) => json,
                        Err(_) => serde_json::Value::String(text),
                    },
                }),
                Err(_) => Err(super::Error::BadStatus {
                    code,
                    body: serde_json::Value::Null,
                }),
            }
        }
    }

    fn request_headers(
        &self,
        mut http_request: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        if let Some(github_token) = &self.github_token {
            http_request = http_request.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", github_token),
            );
        }
        if let Some(user_agent) = &self.user_agent {
            http_request = http_request.header("user-agent", user_agent);
        }
        if let Some(x_title) = &self.x_title {
            http_request = http_request.header("x-title", x_title);
        }
        if let Some(referer) = &self.referer {
            http_request = http_request
                .header("referer", referer)
                .header("http-referer", referer);
        }
        http_request
    }
}
