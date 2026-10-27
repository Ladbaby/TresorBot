package piadapter

import "fmt"

func addUsage(r *Result, message map[string]any) {
	u, ok := message["usage"].(map[string]any)
	if r.Usage == nil {
		r.Usage = &Usage{Models: map[string]ModelUsage{}}
	}
	if !ok {
		r.Usage.Partial = true
		return
	}
	for _, key := range []string{"input", "output", "cacheRead", "cacheWrite"} {
		n, exists := u[key].(float64)
		if !exists || n < 0 {
			r.Usage.Partial = true
			return
		}
	}
	toInt := func(k string) int64 {
		if n, ok := u[k].(float64); ok {
			return int64(n)
		}
		return 0
	}
	provider, _ := message["provider"].(string)
	model, _ := message["model"].(string)
	if r.Usage == nil {
		r.Usage = &Usage{Models: map[string]ModelUsage{}}
	}
	input, output, cr, cw := toInt("input"), toInt("output"), toInt("cacheRead"), toInt("cacheWrite")
	total := toInt("totalTokens")
	if total == 0 {
		total = input + output + cr + cw
	}
	r.Usage.Input += input
	r.Usage.Output += output
	r.Usage.CacheRead += cr
	r.Usage.CacheWrite += cw
	r.Usage.Total += total
	key := provider + "/" + model
	x := r.Usage.Models[key]
	x.Provider, x.Model = provider, model
	x.Input += input
	x.Output += output
	x.CacheRead += cr
	x.CacheWrite += cw
	x.Total += total
	r.Usage.Models[key] = x
}
func importedAssistant(m map[string]any) bool { return m["model"] == "imported-history" }
func handleExtensionUI(r map[string]any, write func(map[string]any) error) error {
	method, _ := r["method"].(string)
	switch method {
	case "notify", "setStatus", "setWidget", "setTitle", "set_editor_text":
		return nil
	case "select", "confirm", "input", "editor":
		id, _ := r["id"].(string)
		return write(map[string]any{"type": "extension_ui_response", "id": id, "cancelled": true})
	default:
		return fmt.Errorf("unknown Pi extension UI method %q", method)
	}
}
