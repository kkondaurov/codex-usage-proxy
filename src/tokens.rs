/// Token-related helper functions used across the tracker.
pub fn blended_total(prompt_tokens: u64, cached_prompt_tokens: u64, completion_tokens: u64) -> u64 {
    let cached = cached_prompt_tokens.min(prompt_tokens);
    prompt_tokens.saturating_sub(cached) + completion_tokens
}

#[cfg(test)]
mod tests {
    use super::blended_total;

    #[test]
    fn blended_total_handles_cached_greater_than_prompt() {
        assert_eq!(blended_total(100, 200, 50), 50);
    }

    #[test]
    fn blended_total_matches_non_cached_plus_output() {
        assert_eq!(blended_total(300, 120, 40), 220);
    }
}
