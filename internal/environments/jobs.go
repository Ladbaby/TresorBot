package environments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"tresorbot/internal/store"
)

// Job provisioning: a bot creating, listing and deleting environment instances.
//
// Why this lives in Go and not in an adapter: a schedule a bot invents must be
// visible to the operator and governed by the same validation, caps and dashboard
// as one they typed. An adapter that kept its own job list would hide that spend
// behind a cursor, and nothing would reconcile it.
//
// The consequence is that `create` is adapter-agnostic: any environment that
// declares the `manage` capability gets working create/list/delete, because the
// instances are Sun's, not the adapter's.

// CapabilityManage marks an adapter whose instances a bot may provision.
//
// Separate from CapabilityTools on purpose. Publishing tools is read-only in
// effect; allowing a bot to create instances lets it cause recurring, paid work
// with no human in the loop, so it is a distinct decision an operator makes by
// installing such an environment at all.
const CapabilityManage = "manage"

// JobLimits bound what a bot may create.
const (
	// MaxBotJobsPerInstance caps instances a single bot may create from one
	// parent instance. A schedule costs a model turn every time it fires, so the
	// limit is about bounded ongoing spend, not storage.
	MaxBotJobsPerInstance = 8
	// MaxJobPromptBytes bounds the prompt a bot writes for a job it creates.
	MaxJobPromptBytes = 4096
)

// Errors a model can act on.
var (
	// ErrJobLimit marks a bot at its ceiling.
	ErrJobLimit = errors.New("job limit reached")
	// ErrJobInvalid marks a job Sun will never accept however often it is retried.
	ErrJobInvalid = errors.New("invalid job")
)

// managedMarkerKey is the setting Sun writes into an instance it created.
//
// It is a setting rather than a column because it must survive a round trip
// through the dashboard's settings editor without a schema change, and because its
// only consumer is a human reading the instance list.
const managedMarkerKey = "created_by_bot"

// JobSpec is what a bot asks for when it creates a schedule.
type JobSpec struct {
	// Name is the instance name the bot suggests. Slug rules apply, and Sun may
	// qualify it to keep it unique.
	Name string
	// Settings are the adapter's own settings, validated against the manifest.
	Settings map[string]any
}

// Job is one provisioned instance as the model sees it.
type Job struct {
	Name     string
	Settings map[string]any
	Created  string
}

// CreateJob provisions a new instance of the same environment as the parent,
// bound to the same bot.
//
// Inheriting the parent's environment and bot is deliberate: it means a bot can
// only create work of a kind the operator already installed for it, so installing
// a cron environment does not silently confer the ability to create webhook
// endpoints.
func (m *Manager) CreateJob(ctx context.Context, parent string, botID string, spec JobSpec) (store.EnvironmentInstance, error) {
	if m == nil {
		return store.EnvironmentInstance{}, ErrUnknownInstance
	}
	parentInstance, err := m.Store.GetEnvironmentInstance(parent)
	if err != nil {
		return store.EnvironmentInstance{}, err
	}
	if !parentInstance.Enabled {
		return store.EnvironmentInstance{}, fmt.Errorf("%w: parent instance %q is disabled", ErrJobInvalid, parent)
	}
	// A bot may only provision for itself, from its own instance.
	if parentInstance.BotID != botID {
		return store.EnvironmentInstance{}, fmt.Errorf("%w: %q does not belong to this bot", ErrJobInvalid, parent)
	}

	info, err := m.manifest(ctx, parentInstance.Environment)
	if err != nil {
		return store.EnvironmentInstance{}, err
	}
	if !hasCapability(info.Capabilities, CapabilityManage) {
		return store.EnvironmentInstance{}, fmt.Errorf("%w: environment %q does not allow a bot to create jobs", ErrJobInvalid, parentInstance.Environment)
	}

	// Validated exactly as the dashboard validates, so a bot cannot store settings
	// an operator could not type.
	normalized, err := NormalizeSettings(Manifest{Name: info.Name, Settings: info.Settings}, spec.Settings, m.Getenv)
	if err != nil {
		return store.EnvironmentInstance{}, fmt.Errorf("%w: %s", ErrJobInvalid, err)
	}
	if prompt, ok := normalized["text"].(string); ok && len(prompt) > MaxJobPromptBytes {
		return store.EnvironmentInstance{}, fmt.Errorf("%w: prompt exceeds %d bytes", ErrJobInvalid, MaxJobPromptBytes)
	}
	// Recorded so the dashboard can show which instances a bot invented, and so the
	// cap below has something durable to count.
	if normalized == nil {
		normalized = map[string]any{}
	}
	normalized[managedMarkerKey] = parentInstance.Name

	existing, err := m.Store.ListEnvironmentInstances()
	if err != nil {
		return store.EnvironmentInstance{}, err
	}
	count := 0
	for _, instance := range existing {
		if instance.BotID == botID && instance.Environment == parentInstance.Environment && markedBy(instance) == parentInstance.Name {
			count++
		}
	}
	if count >= MaxBotJobsPerInstance {
		return store.EnvironmentInstance{}, fmt.Errorf("%w: %d jobs already created from %q (limit %d)", ErrJobLimit, count, parent, MaxBotJobsPerInstance)
	}

	name, err := m.uniqueJobName(spec.Name, parentInstance.Environment, existing)
	if err != nil {
		return store.EnvironmentInstance{}, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return store.EnvironmentInstance{}, err
	}
	instance, err := m.Store.CreateEnvironmentInstance(store.EnvironmentInstance{
		Name:        name,
		Environment: parentInstance.Environment,
		BotID:       botID,
		Enabled:     true,
		Settings:    encoded,
	})
	if err != nil {
		return store.EnvironmentInstance{}, err
	}
	// Reconcile immediately so the new schedule is live before the model is told it
	// succeeded: a job that reports success and fires tomorrow is worse than a slow
	// answer, because the bot will have forgotten by then.
	if err := m.Reconcile(ctx); err != nil {
		m.logf("environment job %q created but reconciliation failed: %v", name, err)
	}
	return instance, nil
}

// ListJobs returns instances the given bot provisioned from this parent.
func (m *Manager) ListJobs(parent string, botID string) ([]Job, error) {
	if m == nil {
		return nil, ErrUnknownInstance
	}
	parentInstance, err := m.Store.GetEnvironmentInstance(parent)
	if err != nil {
		return nil, err
	}
	if parentInstance.BotID != botID {
		return nil, fmt.Errorf("%w: %q does not belong to this bot", ErrJobInvalid, parent)
	}
	instances, err := m.Store.ListEnvironmentInstances()
	if err != nil {
		return nil, err
	}
	jobs := []Job{}
	for _, instance := range instances {
		if instance.BotID != botID || markedBy(instance) != parentInstance.Name {
			continue
		}
		var settings map[string]any
		_ = json.Unmarshal(instance.Settings, &settings)
		delete(settings, managedMarkerKey)
		jobs = append(jobs, Job{Name: instance.Name, Settings: settings, Created: instance.CreatedAt})
	}
	return jobs, nil
}

// DeleteJob removes one provisioned instance.
//
// Only instances a bot created may be deleted this way: an operator's own
// instances are not a bot's to remove, even if the bot is bound to them.
func (m *Manager) DeleteJob(ctx context.Context, parent string, botID string, name string) error {
	if m == nil {
		return ErrUnknownInstance
	}
	parentInstance, err := m.Store.GetEnvironmentInstance(parent)
	if err != nil {
		return err
	}
	if parentInstance.BotID != botID {
		return fmt.Errorf("%w: %q does not belong to this bot", ErrJobInvalid, parent)
	}
	target, err := m.Store.GetEnvironmentInstance(strings.TrimSpace(name))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%w: no job named %q", ErrJobInvalid, name)
		}
		return err
	}
	if target.BotID != botID {
		return fmt.Errorf("%w: job %q belongs to a different bot", ErrJobInvalid, name)
	}
	if markedBy(target) != parentInstance.Name {
		return fmt.Errorf("%w: %q was not created from %q, so a bot may not delete it", ErrJobInvalid, name, parent)
	}
	if err := m.Store.DeleteEnvironmentInstance(target.Name); err != nil {
		return err
	}
	// Stop it now rather than at the next reconcile: a deleted schedule that fires
	// once more is exactly the surprise this whole design is trying to avoid.
	if err := m.Reconcile(ctx); err != nil {
		m.logf("environment job %q deleted but reconciliation failed: %v", name, err)
	}
	return nil
}

// markedBy reports which parent instance created this instance, or "".
func markedBy(instance store.EnvironmentInstance) string {
	var settings map[string]any
	if len(instance.Settings) == 0 || json.Unmarshal(instance.Settings, &settings) != nil {
		return ""
	}
	marker, _ := settings[managedMarkerKey].(string)
	return marker
}

// uniqueJobName derives a valid, unused instance name.
func (m *Manager) uniqueJobName(want string, environment string, existing []store.EnvironmentInstance) (string, error) {
	base := slugify(want)
	if base == "" {
		base = slugify(environment)
	}
	if base == "" {
		base = "job"
	}
	taken := map[string]bool{}
	for _, instance := range existing {
		taken[instance.Name] = true
	}
	for attempt := 0; attempt < 100; attempt++ {
		candidate := base
		if attempt > 0 {
			candidate = fmt.Sprintf("%s-%d", base, attempt+1)
		}
		if !ValidateEnvironmentName(candidate) {
			// Slugify should have handled this; if it did not, truncating is safer
			// than returning a name the store will reject.
			candidate = candidate[:min(len(candidate), 64)]
		}
		if !taken[candidate] {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%w: no free name derived from %q", ErrJobInvalid, want)
}

// slugify reduces a model's suggested name to the instance alphabet.
func slugify(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ' || r == '-' || r == '_' || r == '.':
			b.WriteRune('-')
		}
		if b.Len() >= 48 {
			break
		}
	}
	out := strings.Trim(b.String(), "-")
	for strings.Contains(out, "--") {
		out = strings.ReplaceAll(out, "--", "-")
	}
	return out
}

// hasCapability reports whether an adapter declared one.
func hasCapability(capabilities []string, want string) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

// manifest looks up one adapter's descriptor from the installed catalogue.
func (m *Manager) manifest(ctx context.Context, environment string) (EnvironmentInfo, error) {
	catalogue, err := m.Host.Environments(ctx)
	if err != nil {
		return EnvironmentInfo{}, err
	}
	for _, info := range catalogue {
		if info.Name == environment {
			return info, nil
		}
	}
	return EnvironmentInfo{}, fmt.Errorf("%w: environment %q is not installed", ErrJobInvalid, environment)
}
