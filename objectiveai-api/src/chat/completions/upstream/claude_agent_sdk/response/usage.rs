//! Usage statistics from the Claude Agent SDK subprocess.

use rust_decimal::Decimal;

use super::event::AnthropicUsage;

impl AnthropicUsage {
    /// Transforms Anthropic token usage into the downstream ObjectiveAI format.
    ///
    /// For the Claude Agent SDK, Anthropic is the direct upstream (no intermediary
    /// like OpenRouter), so `upstream_inference_cost == upstream_upstream_inference_cost`.
    pub fn into_downstream(
        self,
        cost: Decimal,
        is_byok: bool,
        cost_multiplier: Decimal,
    ) -> objectiveai::chat::completions::response::Usage {
        let prompt_tokens =
            self.input_tokens + self.cache_creation_input_tokens + self.cache_read_input_tokens;
        let total_tokens = prompt_tokens + self.output_tokens;

        // Anthropic is the only upstream layer — both upstream costs are the raw cost.
        let raw_cost = cost;
        let total_cost = raw_cost * cost_multiplier;

        let (charged_cost, cost_details) = if is_byok {
            (
                total_cost - raw_cost,
                Some(objectiveai::chat::completions::response::CostDetails {
                    upstream_inference_cost: raw_cost,
                    upstream_upstream_inference_cost: raw_cost,
                }),
            )
        } else {
            (total_cost, None)
        };

        objectiveai::chat::completions::response::Usage {
            prompt_tokens,
            completion_tokens: self.output_tokens,
            total_tokens,
            prompt_tokens_details: Some(
                objectiveai::chat::completions::response::PromptTokensDetails {
                    cached_tokens: Some(self.cache_read_input_tokens),
                    cache_write_tokens: Some(self.cache_creation_input_tokens),
                    ..Default::default()
                },
            ),
            completion_tokens_details: None,
            cost: charged_cost,
            cost_details,
            total_cost,
            cost_multiplier,
            is_byok,
        }
    }
}
