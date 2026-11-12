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

// taskUsage maps canonical non-cached input to OpenAI's total prompt
// convention. Missing or partial totals stay unavailable rather than becoming
// zeros.
func taskUsage(raw json.RawMessage) map[string]any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var u struct {
		Input      *int64 `json:"input"`
		Output     *int64 `json:"output"`
		CacheRead  int64  `json:"cache_read"`
		CacheWrite int64  `json:"cache_write"`
		Partial    bool   `json:"partial"`
	}
	if json.Unmarshal(raw, &u) != nil || u.Partial || u.Input == nil || u.Output == nil || *u.Input < 0 || *u.Output < 0 || u.CacheRead < 0 || u.CacheWrite < 0 {
		return nil
	}
	if *u.Input > math.MaxInt64-u.CacheRead || *u.Input+u.CacheRead > math.MaxInt64-u.CacheWrite {
		return nil
	}
	prompt := *u.Input + u.CacheRead + u.CacheWrite
	if prompt > math.MaxInt64-*u.Output {
		return nil
	}
	return map[string]any{"prompt_tokens": prompt, "completion_tokens": *u.Output, "total_tokens": prompt + *u.Output, "prompt_tokens_details": map[string]int64{"cached_tokens": u.CacheRead}, "cache_creation_input_tokens": u.CacheWrite}
}
