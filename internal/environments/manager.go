package environments

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"tresorbot/internal/config"
	"tresorbot/internal/environments/hostjs"
	"tresorbot/internal/store"
)

// Options configures the whole input subsystem for one Sun.
type Options struct {
	// Enabled is the operator's explicit opt-in. Inputs are disabled by default so
	// an existing deployment behaves exactly as it did before this feature: no
	// Node process, no extra listener, no schema writes.
	Enabled bool
	// Store is Sun's database.
	Store *store.Store
	// EnvironmentsDir is the input catalogue. Empty resolves to the default location.
	EnvironmentsDir string
	// StateDir is where the host's own bookkeeping lives.
	StateDir string
	// NodePath runs the host. Empty means "node" from PATH.
	NodePath string
	// TokenSecret derives per-instance ingress credentials. It must be stable,
	// because rotating it invalidates every upstream's configured URL credential.
	TokenSecret string
	// MaxPendingPerBot bounds queued work per bot. Zero uses the default.
	MaxPendingPerBot int
	// Wake nudges the dispatcher after admission.
	Wake func()
	// Logf records subsystem diagnostics.
	Logf func(format string, args ...any)
	// Getenv resolves "env:NAME" secret references.
	Getenv func(string) string
	// Host is injectable for tests; nil builds one from the other options.
	Host *Host
	// Pump and Ingress are injectable for tests.
	Pump    *Pump
	Ingress *Ingress
}

// Manager owns the input subsystem: the host process, the intake pipeline, the
// reply pump and the ingress route.
//
// Its job is composition, not policy. Every decision lives in one place —
// admission in Intake, sending in Pump, authorization in Ingress — so an operator
// reading a log or a test exercising one behaviour is never surprised by another
// layer's rules.
type Manager struct {
	Store            *store.Store
	Host             *Host
	Intake           *Intake
	Pump             *Pump
	Ingress          *Ingress
	TokenSecret      string
	MaxPendingPerBot int
	// Getenv resolves "env:NAME" secret references at configuration time, so a
	// secret exists only in the config frame handed to the host.
	Getenv func(string) string

	tokens sync.Map // instance name -> derived token

	mu       sync.Mutex
	settings map[string]resolvedInstance
	revision map[string]int64

	logf   func(format string, args ...any)
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// resolvedInstance is the cached outcome of validating one configured instance.
type resolvedInstance struct {
	instance Instance
	manifest *Manifest
	settings map[string]any
	revision int64
	err      error
}

// Open builds the subsystem. It does not start the host: a Sun that starts with a
// missing node binary or a broken catalogue must still serve its dashboard.
func Open(o Options) (*Manager, error) {
	if !o.Enabled {
		return nil, nil
	}
	if o.Store == nil {
		return nil, errors.New("inputs require a store")
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.Getenv == nil {
		o.Getenv = func(string) string { return "" }
	}
	if err := o.Store.InitEnvironmentsSchema(); err != nil {
		return nil, fmt.Errorf("inputs schema: %w", err)
	}
	dir := o.EnvironmentsDir
	if dir == "" {
		resolved, err := Dir()
		if err != nil {
			return nil, err
		}
		dir = resolved
	}
	stateDir := o.StateDir
	if stateDir == "" {
		stateDir = dir
	}
	// The host is materialized beside the catalogue rather than inside it, so a
	// catalogue reinstall cannot delete a running host's own modules.
	hostDir := stateDir + "/input-host"
	entry, err := hostjs.Materialize(hostDir)
	if err != nil {
		return nil, fmt.Errorf("materialize input host: %w", err)
	}

	m := &Manager{
		Store:            o.Store,
		TokenSecret:      o.TokenSecret,
		MaxPendingPerBot: o.MaxPendingPerBot,
		Getenv:           o.Getenv,
		settings:         map[string]resolvedInstance{},
		revision:         map[string]int64{},
		logf:             o.Logf,
	}

	host := o.Host
	if host == nil {
		host, err = NewHost(HostConfig{
			NodePath:        o.NodePath,
			Entry:           entry,
			EnvironmentsDir: dir,
			StateDir:        hostDir,
			Logf:            o.Logf,
			OnEvent:         m.onEvent,
			OnCursorGet: func(ctx context.Context, instance, key string) (string, error) {
				return o.Store.EnvironmentCursor(instance, key)
			},
			OnCursorSet: func(ctx context.Context, instance, key, value string) error {
				return o.Store.SetEnvironmentCursor(instance, key, value)
			},
			OnHealth: func(instance string, healthy bool, message string) {
				if !healthy && message != "" {
					o.Logf("input %q is unhealthy: %s", instance, message)
				}
			},
			OnLog: func(instance, level, message string) {
				o.Logf("input %q %s: %s", instance, level, message)
			},
			OnCrash: func(err error) {
				o.Logf("input host crashed: %v", err)
			},
		})
		if err != nil {
			return nil, err
		}
	}
	m.Host = host

	m.Intake = &Intake{
		Store:            intakeStore{store: o.Store, manifests: m},
		MaxPendingPerBot: o.MaxPendingPerBot,
		Wake:             o.Wake,
		Logf:             o.Logf,
		Inspect:          m.instances,
	}
	pump := o.Pump
	if pump == nil {
		pump = NewPump(deliveryStore{store: o.Store}, host)
		pump.Logf = o.Logf
		pump.Routes = m.routeForTask
	}
	m.Pump = pump
	ingress := o.Ingress
	if ingress == nil {
		ingress = &Ingress{Forwarder: host, Instances: m.IngressInstance, Logf: o.Logf}
	}
	m.Ingress = ingress

	// After a host restart Sun re-applies every configured instance, so a crashed
	// adapter resumes from its persisted cursor instead of losing its place.
	host.SetReconcile(m.Reconcile)
	return m, nil
}

// Start begins the reply pump and brings up configured instances.
func (m *Manager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.Pump.Run(ctx)
	}()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.Reconcile(ctx); err != nil {
			m.logf("inputs could not start instances: %v", err)
		}
	}()
}

// Close stops the subsystem. It is bounded by ctx so a wedged adapter cannot hang
// Sun's shutdown.
func (m *Manager) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	if m.Host != nil {
		return m.Host.Close(ctx)
	}
	return nil
}

// Configured reports whether the subsystem is active.
func (m *Manager) Configured() bool { return m != nil }

// IngressPrefix is where webhook ingress is mounted.
const IngressPrefix = "/ingress/"

// Reconcile applies the database's instance configuration to the running host.
//
// It is the only path that starts an instance, and it runs both at boot and after
// every host restart. That single path is what makes recovery uniform: there is no
// separate "bring everything back" code that could drift from the normal one.
func (m *Manager) Reconcile(ctx context.Context) error {
	if m == nil || m.Host == nil {
		return nil
	}
	instances, err := m.Store.ListEnvironmentInstances()
	if err != nil {
		return fmt.Errorf("list input instances: %w", err)
	}
	// Refresh the manifest cache first: an adapter may have been installed or
	// upgraded since the last pass.
	inputs, err := m.Host.Reload(ctx, "")
	if err != nil {
		return fmt.Errorf("reload input catalogue: %w", err)
	}
	byName := map[string]EnvironmentInfo{}
	for _, in := range inputs {
		byName[in.Name] = in
	}
	m.mu.Lock()
	m.settings = map[string]resolvedInstance{}
	m.mu.Unlock()

	var errs []error
	for _, inst := range instances {
		if !inst.Enabled {
			// A disabled instance is stopped rather than left running: the owner
			// turned it off and expects the connection gone.
			if err := m.Host.StopInstance(ctx, inst.Name); err != nil {
				errs = append(errs, fmt.Errorf("stop %s: %w", inst.Name, err))
			}
			continue
		}
		info, ok := byName[inst.Environment]
		if !ok || !info.Healthy {
			reason := "not installed"
			if ok && info.Error != "" {
				reason = info.Error
			}
			m.logf("input instance %q needs input %q, which is %s", inst.Name, inst.Environment, reason)
			continue
		}
		resolved, err := m.resolve(inst, info)
		if err != nil {
			m.mu.Lock()
			m.settings[inst.Name] = resolvedInstance{instance: Instance{Name: inst.Name, Environment: inst.Environment, BotID: inst.BotID, Enabled: false}, err: err}
			m.mu.Unlock()
			m.logf("input instance %q is misconfigured: %v", inst.Name, err)
			continue
		}
		m.mu.Lock()
		m.settings[inst.Name] = resolved
		m.mu.Unlock()
		if err := m.Host.Configure(ctx, ConfigParams{
			Instance:    inst.Name,
			Environment: inst.Environment,
			Settings:    resolved.settings,
			Revision:    resolved.revision,
		}); err != nil {
			errs = append(errs, fmt.Errorf("configure %s: %w", inst.Name, err))
			continue
		}
		if err := m.Host.StartInstance(ctx, inst.Name); err != nil {
			errs = append(errs, fmt.Errorf("start %s: %w", inst.Name, err))
		}
	}
	return errors.Join(errs...)
}

// resolve validates one instance's settings against its manifest.
func (m *Manager) resolve(inst store.EnvironmentInstance, info EnvironmentInfo) (resolvedInstance, error) {
	var values map[string]any
	if len(inst.Settings) > 0 {
		if err := json.Unmarshal(inst.Settings, &values); err != nil {
			return resolvedInstance{}, fmt.Errorf("settings are not a JSON object: %w", err)
		}
	}
	manifest := Manifest{Name: info.Name, Settings: info.Settings}
	normalized, err := NormalizeSettings(manifest, values, m.Getenv)
	if err != nil {
		return resolvedInstance{}, err
	}
	return resolvedInstance{
		instance: Instance{Name: inst.Name, Environment: inst.Environment, BotID: inst.BotID, Enabled: true},
		settings: normalized,
		revision: inst.Revision,
	}, nil
}

// onEvent admits one emitted event. It is called from the host read loop with a
// goroutine per event, so a slow admission cannot stall unrelated traffic.
func (m *Manager) onEvent(ctx context.Context, instance string, raw json.RawMessage) (Ack, error) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return Reject("invalid_event"), &Error{Message: "event is not valid JSON", Reason: CodeBadRequest}
	}
	// The host attributes the event to an instance; a payload that names a
	// different one is refused rather than trusted, so an adapter cannot emit on
	// another's behalf.
	if e.Instance != "" && e.Instance != instance {
		return Reject("instance_mismatch"), &Error{Message: "event instance does not match its source", Reason: CodeBadRequest}
	}
	e.Instance = instance
	return m.Intake.Admit(ctx, e)
}

// instances resolves an instance for intake.
func (m *Manager) instances(name string) (Instance, error) {
	m.mu.Lock()
	resolved, ok := m.settings[name]
	m.mu.Unlock()
	if ok && resolved.err != nil {
		return Instance{}, resolved.err
	}
	if ok {
		return resolved.instance, nil
	}
	// Not yet resolved: read it directly so an event arriving during startup is
	// not silently dropped.
	inst, err := m.Store.GetEnvironmentInstance(name)
	if err != nil {
		return Instance{}, err
	}
	return Instance{Name: inst.Name, Environment: inst.Environment, BotID: inst.BotID, Enabled: inst.Enabled}, nil
}

// routeForTask recovers the reply route an event recorded.
//
// The route lives in the event's payload rather than in a column, so this re-reads
// the original event. It is a fallback: normally the route is already on the
// settled-task row.
func (m *Manager) routeForTask(ctx context.Context, taskID string) (json.RawMessage, error) {
	e, found, err := m.Store.EnvironmentEventForTask(taskID)
	if err != nil || !found {
		return nil, err
	}
	// The stored route is returned verbatim. It used to be reconstructed from the
	// event kind, which silently discarded the conversation: a chat message could be
	// admitted but never answered, because the delivery had no chat to go to. When a
	// source has no route the adapter gets an empty one and decides for itself.
	var route Route
	if e.ReplyTo != "" {
		if err := json.Unmarshal([]byte(e.ReplyTo), &route); err != nil {
			m.logf("instance %q has an unreadable reply route for task %s: %v", e.Instance, taskID, err)
		}
	}
	raw, err := json.Marshal(route)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// deriveToken produces a stable per-instance ingress credential.
//
// It is an HMAC of the instance name under a deployment secret, so it is
// reproducible after a restart without being stored, and knowing one instance's
// token reveals nothing about another's.
func deriveToken(secret, instance string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("tresorbot-ingress\x00"))
	mac.Write([]byte(instance))
	return hex.EncodeToString(mac.Sum(nil))
}

// intakeStore adapts *store.Store to the Intake's narrower interface, filling in
// the manifest lookup the intake needs to validate settings-free admission.
type intakeStore struct {
	store     *store.Store
	manifests *Manager
}

func (s intakeStore) GetEnvironmentInstance(name string) (store.EnvironmentInstance, error) {
	return s.store.GetEnvironmentInstance(name)
}
func (s intakeStore) LookupEnvironmentEvent(instance, eventID string) (store.EnvironmentEvent, bool, error) {
	return s.store.LookupEnvironmentEvent(instance, eventID)
}
func (s intakeStore) RecordEnvironmentEvent(e store.EnvironmentEvent) error {
	return s.store.RecordEnvironmentEvent(e)
}
func (s intakeStore) GetBot(id string) (store.Bot, error) { return s.store.GetBot(id) }

func (s intakeStore) RecordInboundConversation(instance, conversation, label string) error {
	return s.store.RecordInboundConversation(instance, conversation, label)
}
func (s intakeStore) BotByAlias(alias string) (store.Bot, error) {
	return s.store.BotByAlias(alias)
}
func (s intakeStore) ListContacts(botID string) ([]store.Contact, error) {
	return s.store.ListContacts(botID)
}
func (s intakeStore) QueuedTaskCount(botID string) (int, error) {
	return s.store.QueuedTaskCount(botID)
}
func (s intakeStore) AdmitTask(botID, scope, key string, input []byte) (store.Task, bool, error) {
	return s.store.AdmitTask(botID, scope, key, input)
}

// deliveryStore adapts *store.Store to the Pump's narrower interface.
type deliveryStore struct{ store *store.Store }

func (s deliveryStore) SettledEnvironmentTasksAwaitingDelivery(limit int) ([]store.SettledEnvironmentTask, error) {
	return s.store.SettledEnvironmentTasksAwaitingDelivery(limit)
}
func (s deliveryStore) EnvironmentEventForTask(taskID string) (store.EnvironmentEvent, bool, error) {
	return s.store.EnvironmentEventForTask(taskID)
}
func (s deliveryStore) CreateEnvironmentDelivery(instance, taskID, deliveryKey string, route, outcome json.RawMessage) (store.EnvironmentDelivery, bool, error) {
	return s.store.CreateEnvironmentDelivery(instance, taskID, deliveryKey, route, outcome)
}
func (s deliveryStore) DueEnvironmentDeliveries(now time.Time, limit int) ([]store.EnvironmentDelivery, error) {
	return s.store.DueEnvironmentDeliveries(now, limit)
}
func (s deliveryStore) MarkEnvironmentDeliveryDelivered(id string) error {
	return s.store.MarkEnvironmentDeliveryDelivered(id)
}
func (s deliveryStore) MarkEnvironmentDeliveryRetry(id string, attempt int, next time.Time, reason string) error {
	return s.store.MarkEnvironmentDeliveryRetry(id, attempt, next, reason)
}
func (s deliveryStore) MarkEnvironmentDeliveryDead(id string, reason string) error {
	return s.store.MarkEnvironmentDeliveryDead(id, reason)
}

// resolveInputDir reports the catalogue root, exported for the CLI.
func ResolveEnvironmentDir(configured string) (string, error) {
	if strings.TrimSpace(configured) != "" {
		return configured, nil
	}
	return Dir()
}

// OptionsFromConfig builds the subsystem's options from Sun's configuration.
//
// It returns Options with Enabled false when the section is absent, which Open
// turns into a nil Manager. Keeping the translation here rather than in sun.go
// means the config shape and the runtime shape can differ without leaking into
// Sun's composition root.
func OptionsFromConfig(c config.Config, s *store.Store, wake func()) Options {
	o := Options{Store: s, Wake: wake, Logf: log.Printf}
	if c.Environments == nil {
		return o
	}
	o.Enabled = c.Environments.Enabled
	o.EnvironmentsDir = c.Environments.Dir
	o.StateDir = c.Environments.StateDir
	o.NodePath = c.Environments.NodePath
	o.MaxPendingPerBot = c.Environments.MaxPendingPerBot
	o.Getenv = os.Getenv
	// The ingress credential is derived from Sun's own secret material rather than
	// stored, so a database dump cannot leak a token that reaches an upstream. The
	// admin password is the one secret Sun already requires for a non-loopback bind
	// and is stable across restarts; the integration token is preferred when set so
	// rotating the owner password does not silently invalidate every webhook URL.
	switch {
	case c.IntegrationToken != "":
		o.TokenSecret = "integration:" + c.IntegrationToken
	case c.AdminPassword != "":
		o.TokenSecret = "admin:" + c.AdminPassword
	default:
		// A loopback-only Sun with no credentials configured cannot authenticate
		// ingress at all, so webhook inputs stay unreachable rather than open.
		o.TokenSecret = ""
	}
	return o
}
