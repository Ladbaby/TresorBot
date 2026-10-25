package earthui

import (
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"time"
)

//go:embed web/*
var assets embed.FS

const cookieName = "earth_owner_session"

// Sessions is the minimal ownerauth-compatible session surface.
type Sessions interface {
	Issue() (string, error)
	Valid(string) (bool, error)
	Revoke(string) error
}

type Settings struct {
	DefaultWorkspace string `json:"default_workspace"`
	NodePath         string `json:"node_path"`
	PiPath           string `json:"pi_path"`
	PiCLIPath        string `json:"pi_cli_path"`
}

type Options struct {
	Auth             Sessions
	Password         string
	PasswordRequired bool
	Status           func() any
	Load             func() (Settings, string, error)
	Save             func(Settings, string) (string, error)
}

// New returns the Earth minimal UI and API handler. API routes are /api/auth/status,
// /api/auth/login, /api/auth/logout, /api/status, and GET/PUT /api/settings.
func New(opts Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/auth/status", func(w http.ResponseWriter, r *http.Request) {
		csrf, err := csrfCookie(w, r)
		if err != nil {
			http.Error(w, "csrf unavailable", 500)
			return
		}
		authenticated := !opts.PasswordRequired || session(r, opts.Auth) != ""
		if authenticated {
			renew(w, r, opts.Auth)
		}
		writeJSON(w, map[string]any{"password_required": opts.PasswordRequired, "authenticated": authenticated, "csrf_token": csrf})
	})
	mux.HandleFunc("POST /api/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Password string `json:"password"`
		}
		if decode(r, &in) != nil || opts.Auth == nil || subtle.ConstantTimeCompare([]byte(in.Password), []byte(opts.Password)) != 1 {
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		token, err := opts.Auth.Issue()
		if err != nil {
			http.Error(w, "session unavailable", 500)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, Expires: time.Now().Add(365 * 24 * time.Hour)})
		csrf := make([]byte, 32)
		if _, err := rand.Read(csrf); err != nil {
			http.Error(w, "csrf unavailable", 500)
			return
		}
		csrfToken := base64.RawURLEncoding.EncodeToString(csrf)
		http.SetCookie(w, &http.Cookie{Name: "earth_csrf", Value: csrfToken, Path: "/", SameSite: http.SameSiteStrictMode, Expires: time.Now().Add(365 * 24 * time.Hour)})
		writeJSON(w, map[string]any{"csrf_token": csrfToken})
	})
	mux.HandleFunc("POST /api/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		if !validCSRF(r) {
			http.Error(w, "csrf token required", 403)
			return
		}
		if token := session(r, opts.Auth); token != "" {
			if err := opts.Auth.Revoke(token); err != nil {
				http.Error(w, "session unavailable", 503)
				return
			}
		}
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", HttpOnly: true, MaxAge: -1, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("GET /api/status", protect(opts, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opts.Status == nil {
			writeJSON(w, map[string]any{})
			return
		}
		writeJSON(w, opts.Status())
	})))
	mux.Handle("GET /api/settings", protect(opts, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opts.Load == nil {
			http.Error(w, "settings unavailable", 500)
			return
		}
		s, rev, err := opts.Load()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("ETag", rev)
		writeJSON(w, map[string]any{"settings": s, "revision": rev})
	})))
	mux.Handle("PUT /api/settings", protect(opts, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Settings Settings `json:"settings"`
			Revision string   `json:"revision"`
		}
		if decode(r, &in) != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		if opts.Save == nil {
			http.Error(w, "settings unavailable", 500)
			return
		}
		rev, err := opts.Save(in.Settings, in.Revision)
		if err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, ErrConflict) {
				code = http.StatusConflict
			}
			http.Error(w, err.Error(), code)
			return
		}
		writeJSON(w, map[string]string{"revision": rev})
	})))
	web, _ := fs.Sub(assets, "web")
	mux.Handle("/", http.FileServer(http.FS(web)))
	return mux
}

var ErrConflict = errors.New("settings revision conflict")

func protect(opts Options, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := session(r, opts.Auth)
		if t == "" && opts.PasswordRequired {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			if !validCSRF(r) {
				http.Error(w, "csrf token required", 403)
				return
			}
		}
		renew(w, r, opts.Auth)
		next.ServeHTTP(w, r)
	})
}
func session(r *http.Request, auth Sessions) string {
	if auth == nil {
		return ""
	}
	c, e := r.Cookie(cookieName)
	if e != nil || c.Value == "" {
		return ""
	}
	ok, err := auth.Valid(c.Value)
	if err != nil || !ok {
		return ""
	}
	return c.Value
}
func validCSRF(r *http.Request) bool {
	c, e := r.Cookie("earth_csrf")
	return e == nil && c.Value != "" && subtle.ConstantTimeCompare([]byte(c.Value), []byte(r.Header.Get("X-CSRF-Token"))) == 1
}
func csrfCookie(w http.ResponseWriter, r *http.Request) (string, error) {
	if c, e := r.Cookie("earth_csrf"); e == nil && c.Value != "" {
		return c.Value, nil
	}
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		return "", e
	}
	v := base64.RawURLEncoding.EncodeToString(b)
	http.SetCookie(w, &http.Cookie{Name: "earth_csrf", Value: v, Path: "/", SameSite: http.SameSiteStrictMode, MaxAge: 365 * 86400, Secure: r.TLS != nil})
	return v, nil
}
func renew(w http.ResponseWriter, r *http.Request, auth Sessions) {
	v := session(r, auth)
	if v == "" {
		return
	}
	if a, ok := auth.(interface{ Renew(string) error }); ok {
		if a.Renew(v) != nil {
			return
		}
	}
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: v, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: 365 * 86400, Expires: time.Now().Add(365 * 24 * time.Hour), Secure: r.TLS != nil})
}
func decode(r *http.Request, dst any) error {
	d := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
