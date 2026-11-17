package sun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"tresorbot/internal/bottools"
	"tresorbot/internal/environments"
	"tresorbot/internal/store"
)

// environmentBridge gives the environment meta-tools a live catalog and a way to
// run what they find.
//
// It exists rather than exposing the Manager directly for two reasons: bottools
// must not import the environment runtime (the dependency would be circular once
// the manager consults tool grants), and the catalog has to be built from the
// *enabled* instances of *this* Sun, which only the application knows.
type environmentBridge struct {
	app *Application

	mu       sync.Mutex
	catalog  []environments.CatalogEntry
	builtAt  time.Time
	building bool
}

// catalogueMaxAge bounds how stale a catalog may be when nothing signalled a change.
//
// Discovery costs one round trip per enabled instance, so it is not done per turn.
// A short TTL is a safety net rather than the mechanism: start, reload and
// reconfigure all rebuild eagerly, and the TTL only covers an adapter that changed
// its tool set without saying so.
const catalogueMaxAge = 60 * time.Second

// toolCallTimeout bounds one tool call end to end.
//
// Longer than the host's own ceiling so the host reports its timeout first: an
// error naming the adapter is more useful to a model than a bare deadline.
const toolCallTimeout = environments.ToolDefaultTimeout + 5*time.Second

// Catalogue returns the tools currently offered, refreshing when stale.
func (b *environmentBridge) Catalogue() []environments.CatalogEntry {
	b.mu.Lock()
	stale := b.catalog == nil || time.Since(b.builtAt) > catalogueMaxAge
	if !stale || b.building {
		entries := b.catalog
		b.mu.Unlock()
		return entries
	}
	// Only one refresh at a time: several concurrent searches at the start of a
	// turn must not each pay for discovery.
	b.building = true
	b.mu.Unlock()

	entries := b.discover()

	b.mu.Lock()
	b.catalog = entries
	b.builtAt = time.Now()
	b.building = false
	b.mu.Unlock()
	return entries
}

// Refresh rebuilds the catalog eagerly. Called on start and reload so a tool
// enabled or disabled becomes visible before the next search.
func (b *environmentBridge) Refresh() {
	entries := b.discover()
	b.mu.Lock()
	b.catalog = entries
	b.builtAt = time.Now()
	b.mu.Unlock()
}

// discover asks every enabled instance for its tools.
//
// Failures are per instance and logged rather than fatal: one adapter that cannot
// answer must not remove every other environment's tools, which would turn a
// single misbehaving adapter into a system-wide outage of the feature.
func (b *environmentBridge) discover() []environments.CatalogEntry {
	manager := b.app.environments
	if manager == nil {
		return nil
	}
	instances, err := b.app.store.ListEnvironmentInstances()
	if err != nil {
		log.Printf("environment tools: cannot list instances: %v", err)
		return nil
	}
	sources := make([]environments.ToolInstance, 0, len(instances))
	for _, instance := range instances {
		if !instance.Enabled {
			// A disabled instance contributes nothing: its tools would be callable
			// but refused, which is a worse experience than not offering them.
			continue
		}
		sources = append(sources, environments.ToolInstance{Name: instance.Name, Environment: instance.Environment})
	}
	catalog := environments.NewCatalog()
	catalog.Refresh(context.Background(), manager.Host, sources)
	for instance, problem := range catalog.Problems() {
		log.Printf("environment tools: %s: %s", instance, problem)
	}
	return catalog.Entries()
}

// Call runs one tool on behalf of a bot.
//
// The bot is passed through so the grant check happens in one place (bottools),
// but it is re-checked against the store here as well: this method is the last
// point before third-party code runs, and an authorization decision made earlier
// in a longer call chain is not something to trust at the boundary.
func (b *environmentBridge) Call(ctx context.Context, botID string, entry environments.CatalogEntry, arguments json.RawMessage) (json.RawMessage, error) {
	manager := b.app.environments
	if manager == nil {
		return nil, bottools.ErrNoEnvironments
	}
	instance, err := b.app.store.GetEnvironmentInstance(entry.Instance)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, fmt.Errorf("environment instance %q is no longer configured", entry.Instance)
		}
		return nil, err
	}
	if !instance.Enabled {
		return nil, fmt.Errorf("environment instance %q is disabled", entry.Instance)
	}
	if instance.BotID != botID {
		return nil, fmt.Errorf("environment instance %q belongs to a different bot", entry.Instance)
	}

	callCtx, cancel := context.WithTimeout(ctx, toolCallTimeout)
	defer cancel()
	// One id per call attempt. The model never sees it; the adapter may use it to
	// recognise a retry rather than acting twice.
	callID := entry.ID + "@" + time.Now().UTC().Format("20060102T150405.000000000")
	result, err := manager.Host.CallTool(callCtx, environments.ToolsCallParams{
		Instance:  entry.Instance,
		Name:      entry.Tool,
		Arguments: arguments,
		CallID:    callID,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// newTools builds a bottools handler wired to the environment subsystem.
//
// A single constructor for every call site, because the handler must always carry
// the bridge: a handler built without it would answer TresorBotToolSearch with
// "no environments are enabled" even when they are, which reads to a model as a
// permanent absence rather than a misconfiguration.
func (a *Application) newTools() *bottools.Handler {
	handler := bottools.New(a.store, a.memory)
	if a.environments != nil {
		if a.bridge == nil {
			a.bridge = &environmentBridge{app: a}
		}
		handler.WithEnvironment(a.bridge)
	}
	return handler
}
