package bridge

import (
	"encoding/json"
	"math"
)

// includeUsage honors the client's standard SSE usage preference.
func includeUsage(raw json.RawMessage) bool {
	decoded, err := DecodeBody(raw)
	if err != nil {
		return false
	}
	raw = decoded
	var input struct {
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	return json.Unmarshal(raw, &input) == nil && input.StreamOptions.IncludeUsage
}

// taskUsage maps Pi's non-cached input to OpenAI's total prompt convention.
// Missing or partial totals stay unavailable rather than becoming zeros.
func taskUsage(raw json.RawMessage) map[string]any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var pi struct {
		Input      *int64 `json:"input"`
		Output     *int64 `json:"output"`
		CacheRead  int64  `json:"cacheRead"`
		CacheWrite int64  `json:"cacheWrite"`
		Partial    bool   `json:"partial"`
	}
	if json.Unmarshal(raw, &pi) != nil || pi.Partial || pi.Input == nil || pi.Output == nil || *pi.Input < 0 || *pi.Output < 0 || pi.CacheRead < 0 || pi.CacheWrite < 0 {
		return nil
	}
	if *pi.Input > math.MaxInt64-pi.CacheRead || *pi.Input+pi.CacheRead > math.MaxInt64-pi.CacheWrite {
		return nil
	}
	prompt := *pi.Input + pi.CacheRead + pi.CacheWrite
	if prompt > math.MaxInt64-*pi.Output {
		return nil
	}
	return map[string]any{"prompt_tokens": prompt, "completion_tokens": *pi.Output, "total_tokens": prompt + *pi.Output, "prompt_tokens_details": map[string]int64{"cached_tokens": pi.CacheRead}, "cache_creation_input_tokens": pi.CacheWrite}
}
