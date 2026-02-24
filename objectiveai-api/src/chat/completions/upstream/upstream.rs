//! Upstream provider enumeration.

/// Returns an iterator over available upstream providers for a request.
pub fn upstreams(
    ensemble_llm: &objectiveai::ensemble_llm::EnsembleLlm,
    request: &super::Params,
) -> Vec<objectiveai::chat::completions::Upstream> {
    if ensemble_llm.base.model.starts_with("anthropic/")
        && (ensemble_llm
            .base
            .provider
            .as_ref()
            .is_none_or(|p| p.require_parameters.is_none_or(|r| !r)))
    {
        upstreams_filtered(
            &[
                objectiveai::chat::completions::Upstream::ClaudeAgentSdk,
                objectiveai::chat::completions::Upstream::OpenRouter,
            ],
            request,
        )
    } else {
        upstreams_filtered(
            &[objectiveai::chat::completions::Upstream::OpenRouter],
            request,
        )
    }
}

fn upstreams_filtered(
    from_upstreams: &[objectiveai::chat::completions::Upstream],
    request: &super::Params,
) -> Vec<objectiveai::chat::completions::Upstream> {
    from_upstreams
        .iter()
        .filter(|upstream| match upstream {
            objectiveai::chat::completions::Upstream::ClaudeAgentSdk => {
                request.upstreams().map_or(true, |ups| {
                    ups.contains(&objectiveai::chat::completions::Upstream::ClaudeAgentSdk)
                })
            }
            objectiveai::chat::completions::Upstream::OpenRouter => {
                request.upstreams().map_or(true, |ups| {
                    ups.contains(&objectiveai::chat::completions::Upstream::OpenRouter)
                })
            }
            objectiveai::chat::completions::Upstream::Unknown => false,
        })
        .cloned()
        .collect()
}
