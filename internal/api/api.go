package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"tresorbot/internal/memory"
	"tresorbot/internal/store"
)

type Options struct {
	AdminPassword, BindAddr, MemoryRoot string
	RunnerStatus                        func(string) string
	RunnerChanged                       func(string)
	TasksChanged                        func()
}
type API struct {
	s        *store.Store
	m        *memory.Store
	o        Options
	mu       sync.Mutex
	sessions map[string]bool
}

func New(s *store.Store, m *memory.Store, o Options) http.Handler {
	a := &API{s: s, m: m, o: o, sessions: map[string]bool{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/", a.route)
	mux.Handle("/", ui())
	return mux
}
func (a *API) loopback() bool {
	host, _, e := net.SplitHostPort(a.o.BindAddr)
	if e != nil {
		return false
	}
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}
func (a *API) route(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/health" {
		if r.Method != "GET" {
			writeErr(w, 405, "method not allowed")
			return
		}
		if e := a.s.Ping(); e != nil {
			writeErr(w, 503, "unavailable")
			return
		}
		jsonOut(w, 200, map[string]any{"ok": true})
		return
	}
	if r.URL.Path == "/api/auth/status" {
		if r.Method != "GET" {
			writeErr(w, 405, "method not allowed")
			return
		}
		csrf := cookie(r, "owner_csrf")
		if csrf == "" {
			csrf = token()
		}
		http.SetCookie(w, &http.Cookie{Name: "owner_csrf", Value: csrf, Path: "/", SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil})
		jsonOut(w, 200, map[string]any{"authenticated": a.auth(r), "password_required": a.o.AdminPassword != "" || !a.loopback(), "csrf_token": csrf})
		return
	}
	if r.URL.Path == "/api/auth/login" && r.Method == "POST" {
		if !a.origin(r) {
			writeErr(w, 403, "origin rejected")
			return
		}
		var v struct {
			Password string `json:"password"`
		}
		if !decode(w, r, &v) {
			return
		}
		if a.o.AdminPassword != "" && subtle.ConstantTimeCompare([]byte(v.Password), []byte(a.o.AdminPassword)) != 1 {
			writeErr(w, 401, "invalid credentials")
			return
		}
		if a.o.AdminPassword == "" && !a.loopback() {
			writeErr(w, 403, "password required")
			return
		}
		id := token()
		a.mu.Lock()
		a.sessions[id] = true
		a.mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "owner_session", Value: id, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil})
		jsonOut(w, 200, map[string]bool{"ok": true})
		return
	}
	if r.URL.Path == "/api/auth/logout" && r.Method == "POST" {
		if !a.mutation(r) {
			writeErr(w, 403, "request rejected")
			return
		}
		a.mu.Lock()
		delete(a.sessions, cookie(r, "owner_session"))
		a.mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "owner_session", Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
		jsonOut(w, 200, map[string]bool{"ok": true})
		return
	}
	if !a.auth(r) {
		writeErr(w, 401, "authentication required")
		return
	}
	if mutation(r.Method) && !a.mutation(r) {
		writeErr(w, 403, "request rejected")
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/api/")
	parts := strings.Split(p, "/")
	switch parts[0] {
	case "bots":
		a.bots(w, r, parts)
	case "tasks":
		a.tasks(w, r, parts)
	case "runners":
		a.runners(w, r, parts)
	case "memory":
		a.mem(w, r)
	case "settings":
		if r.Method == "GET" {
			jsonOut(w, 200, map[string]any{"bind_addr": a.o.BindAddr, "memory_root": a.o.MemoryRoot, "retention": "records are retained", "runner_transport": "not connected"})
		} else {
			writeErr(w, 405, "method not allowed")
		}
	case "events":
		a.events(w, r)
	default:
		writeErr(w, 404, "not found")
	}
}
func token() string { b := make([]byte, 32); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func cookie(r *http.Request, n string) string {
	c, e := r.Cookie(n)
	if e != nil {
		return ""
	}
	return c.Value
}
func (a *API) auth(r *http.Request) bool {
	if a.o.AdminPassword == "" && a.loopback() {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sessions[cookie(r, "owner_session")]
}
func (a *API) origin(r *http.Request) bool {
	u, err := url.Parse(r.Header.Get("Origin"))
	if err != nil || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return u.Scheme == scheme && strings.EqualFold(u.Host, r.Host)
}
func (a *API) mutation(r *http.Request) bool {
	if !a.origin(r) {
		return false
	}
	c, e := r.Cookie("owner_csrf")
	return e == nil && c.Value != "" && subtle.ConstantTimeCompare([]byte(c.Value), []byte(r.Header.Get("X-CSRF-Token"))) == 1
}
func mutation(m string) bool { return m != "GET" && m != "HEAD" && m != "OPTIONS" }
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		writeErr(w, 400, "invalid request")
		return false
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		writeErr(w, 400, "invalid request")
		return false
	}
	return true
}
func jsonOut(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeErr(w http.ResponseWriter, status int, s string) {
	code := "request_failed"
	switch status {
	case 400:
		code = "invalid_request"
	case 401:
		code = "authentication_required"
	case 403:
		code = "forbidden"
	case 404:
		code = "not_found"
	case 405:
		code = "method_not_allowed"
	case 409:
		code = "conflict"
	case 500:
		code = "internal_error"
	case 503:
		code = "unavailable"
	}
	jsonOut(w, status, map[string]any{"error": s, "code": code, "message": s, "retryable": status == 503})
}
func errStatus(e error) int {
	if errors.Is(e, store.ErrDisabled) {
		return 409
	}
	if errors.Is(e, store.ErrNotFound) || errors.Is(e, memory.ErrNotFound) {
		return 404
	}
	if errors.Is(e, store.ErrConflict) || errors.Is(e, memory.ErrConflict) {
		return 409
	}
	if errors.Is(e, store.ErrInvalid) || errors.Is(e, memory.ErrInvalid) {
		return 400
	}
	return 500
}
func respond(w http.ResponseWriter, v any, e error) {
	if e != nil {
		if status := errStatus(e); status == 500 {
			writeErr(w, 500, "internal service error")
		} else {
			writeErr(w, status, e.Error())
		}
		return
	}
	jsonOut(w, 200, v)
}
func (a *API) bots(w http.ResponseWriter, r *http.Request, p []string) {
	if r.Method != "GET" && a.o.TasksChanged != nil {
		defer a.o.TasksChanged()
	}
	if len(p) == 1 {
		switch r.Method {
		case "GET":
			v, e := a.s.ListBots()
			respond(w, v, e)
			return
		case "POST":
			var b store.Bot
			if !decode(w, r, &b) {
				return
			}
			v, e := a.s.CreateBot(b)
			respond(w, v, e)
			return
		}
	}
	if len(p) == 2 {
		switch r.Method {
		case "GET":
			v, e := a.s.GetBot(p[1])
			respond(w, v, e)
			return
		case "PUT":
			var q struct {
				Revision     int64   `json:"revision"`
				Alias        *string `json:"alias"`
				Name         *string `json:"name"`
				Instructions *string `json:"instructions"`
				RunnerID     *string `json:"runner_id"`
				Workspace    *string `json:"workspace"`
				Adapter      *string `json:"adapter"`
				SharedWrite  *bool   `json:"shared_write"`
				Enabled      *bool   `json:"enabled"`
			}
			if !decode(w, r, &q) {
				return
			}
			v, e := a.s.UpdateBot(p[1], q.Revision, store.BotPatch{Alias: q.Alias, Name: q.Name, Instructions: q.Instructions, RunnerID: q.RunnerID, Workspace: q.Workspace, Adapter: q.Adapter, SharedWrite: q.SharedWrite, Enabled: q.Enabled})
			respond(w, v, e)
			return
		case "DELETE":
			e := a.s.DeleteBot(p[1])
			respond(w, map[string]bool{"ok": true}, e)
			return
		}
	}
	if len(p) == 3 && p[2] == "contacts" {
		switch r.Method {
		case "GET":
			v, e := a.s.ListContacts(p[1])
			respond(w, v, e)
			return
		case "PUT":
			var q struct {
				Contacts []store.Contact `json:"contacts"`
			}
			if !decode(w, r, &q) {
				return
			}
			respond(w, map[string]bool{"ok": true}, a.s.ReplaceContacts(p[1], q.Contacts))
			return
		}
	}
	writeErr(w, 405, "method not allowed")
}
func optionalInt(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.Atoi(value)
}

func (a *API) tasks(w http.ResponseWriter, r *http.Request, p []string) {
	if r.Method != "GET" && a.o.TasksChanged != nil {
		defer a.o.TasksChanged()
	}
	if len(p) == 1 {
		if r.Method == "GET" {
			lim, err := optionalInt(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, 400, "invalid limit")
				return
			}
			off, err := optionalInt(r.URL.Query().Get("offset"))
			if err != nil {
				writeErr(w, 400, "invalid offset")
				return
			}
			v, e := a.s.ListTasks(r.URL.Query().Get("bot_id"), lim, off)
			respond(w, v, e)
			return
		}
		if r.Method == "POST" {
			var q struct {
				BotID string `json:"bot_id"`
				Text  string `json:"text"`
			}
			if !decode(w, r, &q) {
				return
			}
			if strings.TrimSpace(q.Text) == "" {
				writeErr(w, 400, "task text is required")
				return
			}
			b, _ := json.Marshal(map[string]string{"text": q.Text})
			v, _, e := a.s.AdmitTask(q.BotID, "owner", r.Header.Get("Idempotency-Key"), b)
			respond(w, v, e)
			return
		}
	}
	if len(p) == 2 {
		switch r.Method {
		case "GET":
			v, e := a.s.GetTask(p[1])
			respond(w, v, e)
			return
		case "DELETE":
			e := a.s.CancelTask(p[1])
			respond(w, map[string]bool{"ok": true}, e)
			return
		}
	}
	if len(p) == 3 && p[2] == "retry" && r.Method == "POST" {
		v, e := a.s.RetryTask(p[1])
		respond(w, v, e)
		return
	}
	if len(p) == 3 && p[2] == "abandon" && r.Method == "POST" {
		var q struct {
			Confirmed bool `json:"confirmed"`
		}
		if !decode(w, r, &q) {
			return
		}
		e := a.s.AbandonTask(p[1], q.Confirmed)
		respond(w, map[string]bool{"ok": true}, e)
		return
	}
	if len(p) <= 2 || (len(p) == 3 && (p[2] == "retry" || p[2] == "abandon")) {
		writeErr(w, 405, "method not allowed")
		return
	}
	writeErr(w, 404, "not found")
}
func (a *API) runners(w http.ResponseWriter, r *http.Request, p []string) {
	if len(p) == 1 && r.Method == "GET" {
		v, e := a.s.ListRunners()
		if e != nil {
			respond(w, nil, e)
			return
		}
		type inventory struct {
			store.Runner
			Status string `json:"status"`
		}
		out := make([]inventory, 0, len(v))
		for _, r := range v {
			status := "disconnected"
			if r.Revoked {
				status = "revoked"
			} else if a.o.RunnerStatus != nil {
				status = a.o.RunnerStatus(r.ID)
			}
			out = append(out, inventory{r, status})
		}
		respond(w, out, nil)
		return
	}
	if len(p) == 1 && r.Method == "POST" {
		var q struct {
			Name string `json:"name"`
		}
		if !decode(w, r, &q) {
			return
		}
		id, e := a.s.CreateRunner(q.Name)
		respond(w, map[string]string{"id": id, "name": q.Name, "status": "disconnected"}, e)
		return
	}
	if len(p) == 3 && p[2] == "credential" && r.Method == "POST" {
		credential, generation, e := a.s.IssueRunnerCredential(p[1])
		if e == nil && a.o.RunnerChanged != nil {
			a.o.RunnerChanged(p[1])
		}
		w.Header().Set("Cache-Control", "no-store")
		respond(w, map[string]any{"runner_id": p[1], "runner_token": credential, "generation": generation}, e)
		return
	}
	if len(p) == 3 && p[2] == "revoke" && r.Method == "POST" {
		e := a.s.RevokeRunner(p[1])
		if e == nil && a.o.RunnerChanged != nil {
			a.o.RunnerChanged(p[1])
		}
		respond(w, map[string]bool{"ok": true}, e)
		return
	}
	if len(p) == 1 || (len(p) == 3 && (p[2] == "revoke" || p[2] == "credential")) {
		writeErr(w, 405, "method not allowed")
		return
	}
	writeErr(w, 404, "not found")
}
func (a *API) mem(w http.ResponseWriter, r *http.Request) {
	if a.m == nil {
		writeErr(w, 503, "memory unavailable")
		return
	}
	scope, bot := r.URL.Query().Get("scope"), r.URL.Query().Get("bot_id")
	if scope == "" {
		scope = "shared"
	}
	if r.Method == "GET" {
		p := r.URL.Query().Get("path")
		if p == "" {
			v, e := a.m.List(scope, bot)
			respond(w, v, e)
		} else {
			v, e := a.m.Read(scope, bot, p)
			respond(w, v, e)
		}
		return
	}
	if r.Method == "PUT" {
		var q struct {
			Path             string `json:"path"`
			ExpectedRevision string `json:"expected_revision"`
			Content          string `json:"content"`
			OperationID      string `json:"operation_id"`
		}
		if !decode(w, r, &q) {
			return
		}
		v, e := a.m.Write(scope, bot, q.Path, q.ExpectedRevision, q.Content, q.OperationID)
		respond(w, v, e)
		return
	}
	writeErr(w, 405, "method not allowed")
}
func (a *API) events(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeErr(w, 405, "method not allowed")
		return
	}
	cur := int64(0)
	if header := r.Header.Get("Last-Event-ID"); header != "" {
		var err error
		cur, err = strconv.ParseInt(header, 10, 64)
		if err != nil || cur < 0 {
			writeErr(w, 400, "invalid event cursor")
			return
		}
	}
	f, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, 500, "streaming unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	for i := 0; i < 60; i++ {
		ev, e := a.s.ReadOwnerEvents(cur, r.URL.Query().Get("task_id"), 100)
		if e != nil {
			return
		}
		for _, x := range ev {
			b, _ := json.Marshal(x)
			_, _ = w.Write([]byte("id: " + strconv.FormatInt(x.Cursor, 10) + "\ndata: " + string(b) + "\n\n"))
			cur = x.Cursor
		}
		f.Flush()
		if len(ev) == 100 {
			continue
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Second):
		}
	}
}
