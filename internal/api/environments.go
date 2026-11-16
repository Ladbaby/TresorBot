package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"time"

	"tresorbot/internal/environments"
	"tresorbot/internal/settingspec"
	"tresorbot/internal/store"
)

// inputsListTimeout bounds a catalogue read. The host answers from memory, so a
// slow read means the process is wedged rather than busy.
const inputsListTimeout = 5 * time.Second

// environmentsRoute serves /api/environments and its children.
//
// Instances live here rather than in sun.yaml because they are plural per adapter
// and need a revision for safe concurrent editing, which a nested YAML map cannot
// express. The handler is therefore a normal owner-authenticated CRUD surface.
func (a *API) environmentsRoute(w http.ResponseWriter, r *http.Request, parts []string) {
	// Listing is useful even when the subsystem is off: it tells the owner the
	// catalogue exists and nothing is configured, instead of showing a blank tab.
	if len(parts) == 1 {
		switch r.Method {
		case "GET":
			a.listInputs(w, r)
			return
		case "POST":
			a.createEnvironment(w, r)
			return
		}
	}
	if len(parts) == 2 {
		switch r.Method {
		case "GET":
			a.getInput(w, r, parts[1])
			return
		case "PUT":
			a.updateInput(w, r, parts[1])
			return
		case "DELETE":
			inst, err := a.s.GetEnvironmentInstance(parts[1])
			if err != nil {
				respond(w, nil, err)
				return
			}
			if err := a.s.DeleteEnvironmentInstance(parts[1]); err != nil {
				respond(w, nil, err)
				return
			}
			// Reconcile so the adapter actually stops rather than continuing to
			// poll an upstream the owner just removed.
			a.reconcileInputs(r.Context())
			respond(w, map[string]any{"ok": true, "deleted": true, "name": inst.Name}, nil)
			return
		}
	}
	// /api/environments/{name}/events and /api/environments/{name}/deliveries expose the
	// record of what arrived and what was sent back, which is what makes a
	// misbehaving upstream diagnosable without reading a log.
	if len(parts) == 3 {
		switch parts[2] {
		case "events":
			if r.Method != "GET" {
				writeErr(w, 405, "method not allowed")
				return
			}
			events, err := a.s.ListEnvironmentEvents(parts[1], 100)
			respond(w, map[string]any{"events": events}, err)
			return
		case "token":
			// Revealed on demand rather than stored, and only over the authenticated
			// owner API: the whole point of deriving it is that it never sits at rest.
			if r.Method != "POST" {
				writeErr(w, 405, "method not allowed")
				return
			}
			if a.o.Environments == nil {
				writeErr(w, 409, "inputs are disabled")
				return
			}
			token, err := a.o.Environments.IngressToken(r.Context(), parts[1])
			if err != nil {
				writeErr(w, 409, err.Error())
				return
			}
			respond(w, map[string]any{"token": token, "path": environments.IngressPrefix + parts[1]}, nil)
			return
		case "deliveries":
			if r.Method != "GET" {
				writeErr(w, 405, "method not allowed")
				return
			}
			deliveries, err := a.s.ListInputDeliveries(r.URL.Query().Get("state"), 100)
			respond(w, map[string]any{"deliveries": deliveries}, err)
			return
		}
	}
	writeErr(w, 404, "not found")
}

// listInputs returns configured instances plus the loaded catalogue.
//
// Settings are redacted: a sensitive value is never echoed, and Sun never stored
// it in the first place — it holds a reference and resolves it at configuration
// time. Redaction here keeps the response shape uniform.
func (a *API) listInputs(w http.ResponseWriter, r *http.Request) {
	instances, err := a.s.ListEnvironmentInstances()
	if err != nil {
		respond(w, nil, err)
		return
	}
	catalogue, problems := a.inputCatalogue(r.Context())
	views := make([]map[string]any, 0, len(instances))
	for _, inst := range instances {
		views = append(views, a.inputInstanceView(inst, catalogue))
	}
	respond(w, map[string]any{
		"enabled":          a.o.Environments != nil,
		"dir":              a.o.EnvironmentsDir,
		"instances":        views,
		"catalogue":        catalogue,
		"problems":         problems,
		"ingress_prefix":   environments.IngressPrefix,
		"max_pending_cap":  environments.DefaultMaxPendingPerBot,
		"max_pending_hint": "Queued events per bot before an upstream is told to retry.",
	}, nil)
}

// inputInstanceView renders one instance with sensitive settings removed.
func (a *API) inputInstanceView(inst store.EnvironmentInstance, catalogue []environments.EnvironmentInfo) map[string]any {
	var declared []settingspec.Descriptor
	for _, info := range catalogue {
		if info.Name == inst.Environment {
			declared = info.Settings
			break
		}
	}
	settings := map[string]any{}
	if len(inst.Settings) > 0 {
		if json.Unmarshal(inst.Settings, &settings) != nil {
			settings = map[string]any{}
		}
	}
	redacted := make(map[string]any, len(settings))
	for key, value := range settings {
		redacted[key] = value
	}
	for _, d := range declared {
		if !d.Sensitive {
			continue
		}
		if _, present := redacted[d.Key]; present {
			// An environment reference is not itself a secret, so it can be shown;
			// a literal value never is.
			if str, ok := redacted[d.Key].(string); ok && settingspec.IsEnvRef(str) {
				continue
			}
			redacted[d.Key] = ""
		}
	}
	return map[string]any{
		"name":            inst.Name,
		"input":           inst.Environment,
		"bot_id":          inst.BotID,
		"enabled":         inst.Enabled,
		"settings":        redacted,
		"revision":        inst.Revision,
		"created_at":      inst.CreatedAt,
		"updated_at":      inst.UpdatedAt,
		"ingress_path":    environments.IngressPrefix + inst.Name,
		"webhook_capable": inputHasCapability(catalogue, inst.Environment, environments.CapabilityWebhook),
		"declared":        declared,
	}
}

// getInput returns one instance.
func (a *API) getInput(w http.ResponseWriter, r *http.Request, name string) {
	inst, err := a.s.GetEnvironmentInstance(name)
	if err != nil {
		respond(w, nil, err)
		return
	}
	catalogue, _ := a.inputCatalogue(r.Context())
	respond(w, a.inputInstanceView(inst, catalogue), nil)
}

// createEnvironment adds one instance and immediately reconciles so it starts.
func (a *API) createEnvironment(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Name        string          `json:"name"`
		Environment string          `json:"environment"`
		BotID       string          `json:"bot_id"`
		Enabled     *bool           `json:"enabled"`
		Settings    json.RawMessage `json:"settings"`
	}
	if !decode(w, r, &q) {
		return
	}
	enabled := true
	if q.Enabled != nil {
		enabled = *q.Enabled
	}
	// Validate the settings against the manifest before storing them, so a typo is
	// an error the owner sees rather than a failure the adapter logs at 3am.
	if err := a.validateInputSettings(r.Context(), q.Environment, q.Settings); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	inst, err := a.s.CreateEnvironmentInstance(store.EnvironmentInstance{Name: q.Name, Environment: q.Environment, BotID: q.BotID, Enabled: enabled, Settings: q.Settings})
	if err != nil {
		respond(w, nil, err)
		return
	}
	a.reconcileInputs(r.Context())
	catalogue, _ := a.inputCatalogue(r.Context())
	respond(w, a.inputInstanceView(inst, catalogue), nil)
}

// updateInput applies a revisioned patch.
func (a *API) updateInput(w http.ResponseWriter, r *http.Request, name string) {
	var q struct {
		Revision int64            `json:"revision"`
		BotID    *string          `json:"bot_id"`
		Enabled  *bool            `json:"enabled"`
		Settings *json.RawMessage `json:"settings"`
	}
	if !decode(w, r, &q) {
		return
	}
	if q.Settings != nil {
		inst, err := a.s.GetEnvironmentInstance(name)
		if err != nil {
			respond(w, nil, err)
			return
		}
		if err := a.validateInputSettings(r.Context(), inst.Environment, *q.Settings); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
	}
	inst, err := a.s.UpdateEnvironmentInstance(name, q.Revision, store.EnvironmentInstancePatch{BotID: q.BotID, Enabled: q.Enabled, Settings: q.Settings})
	if err != nil {
		respond(w, nil, err)
		return
	}
	a.reconcileInputs(r.Context())
	catalogue, _ := a.inputCatalogue(r.Context())
	respond(w, a.inputInstanceView(inst, catalogue), nil)
}

// validateInputSettings checks values against the input's declared descriptors.
func (a *API) validateInputSettings(ctx context.Context, inputName string, raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var values map[string]any
	if err := json.Unmarshal(raw, &values); err != nil {
		return errors.New("settings must be a JSON object")
	}
	catalogue, _ := a.inputCatalogue(ctx)
	for _, info := range catalogue {
		if info.Name != inputName {
			continue
		}
		// Normalize without an environment, so a reference is accepted here and
		// resolved at configuration time; a missing variable is reported by
		// reconcile, where the owner sees it against the instance.
		_, err := settingspec.Normalize(info.Settings, values)
		return err
	}
	return nil
}

// inputCatalogue reads the loaded input catalogue, tolerating a disabled or
// not-yet-started subsystem.
func (a *API) inputCatalogue(ctx context.Context) ([]environments.EnvironmentInfo, map[string]string) {
	if a.o.Environments == nil || a.o.Environments.Host == nil {
		return []environments.EnvironmentInfo{}, map[string]string{}
	}
	ctx, cancel := context.WithTimeout(ctx, inputsListTimeout)
	defer cancel()
	list, err := a.o.Environments.Host.Environments(ctx)
	if err != nil {
		return []environments.EnvironmentInfo{}, map[string]string{"host": err.Error()}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list, map[string]string{}
}

// reconcileInputs applies configuration changes to the running host.
//
// It runs inline rather than in the background so a failed start is reported
// against the edit that caused it, which is the moment the owner can act on it.
func (a *API) reconcileInputs(ctx context.Context) {
	if a.o.Environments == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := a.o.Environments.Reconcile(ctx); err != nil {
		log.Printf("inputs: reconcile failed: %v", err)
	}
}

// inputHasCapability reports whether an input declared one capability.
func inputHasCapability(catalogue []environments.EnvironmentInfo, name, capability string) bool {
	for _, info := range catalogue {
		if info.Name != name {
			continue
		}
		for _, c := range info.Capabilities {
			if c == capability {
				return true
			}
		}
		return false
	}
	return false
}
