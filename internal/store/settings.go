package store

// Stream settings control response content shaping for streaming chats.
// Absent rows mean the defaults: heartbeats on, intermediate turns hidden, and
// the default progress wording.
const (
	settingHeartbeat        = "stream_heartbeat"
	settingShowIntermediate = "stream_show_intermediate"
	settingActivity         = "stream_activity"
	defaultHeartbeat        = true
	defaultShowIntermediate = false
	// DefaultActivity is the progress wording used until the owner sets one.
	// {label} is replaced with the step being performed (a tool name such as
	// TresorBotSendMessage); {tool} is the raw tool name.
	DefaultActivity = "Waiting for {label}\u2026"
)

// StreamConfig is the service-level streaming content configuration.
type StreamConfig struct {
	Heartbeat        bool
	ShowIntermediate bool
	// Activity is the progress template shown while the bot works.
	Activity string
}

// StreamSettings returns the service-level streaming content options.
func (s *Store) StreamSettings() (StreamConfig, error) {
	cfg := StreamConfig{Heartbeat: defaultHeartbeat, ShowIntermediate: defaultShowIntermediate, Activity: DefaultActivity}
	rows, err := s.db.Query(`SELECT key, value, value_text FROM settings WHERE key IN(?, ?, ?)`, settingHeartbeat, settingShowIntermediate, settingActivity)
	if err != nil {
		return StreamConfig{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var key, text string
		var value int
		if err = rows.Scan(&key, &value, &text); err != nil {
			return StreamConfig{}, err
		}
		switch key {
		case settingHeartbeat:
			cfg.Heartbeat = value != 0
		case settingShowIntermediate:
			cfg.ShowIntermediate = value != 0
		case settingActivity:
			if text != "" {
				cfg.Activity = text
			}
		}
	}
	if err = rows.Err(); err != nil {
		return StreamConfig{}, err
	}
	return cfg, nil
}

// SetStreamSettings stores the service-level streaming content options.
func (s *Store) SetStreamSettings(cfg StreamConfig) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	flags := map[string]bool{settingHeartbeat: cfg.Heartbeat, settingShowIntermediate: cfg.ShowIntermediate}
	for key, value := range flags {
		v := 0
		if value {
			v = 1
		}
		if _, err = tx.Exec(`INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, v); err != nil {
			return err
		}
	}
	activity := cfg.Activity
	if activity == "" {
		activity = DefaultActivity
	}
	if _, err = tx.Exec(`INSERT INTO settings(key, value, value_text) VALUES(?, 1, ?) ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text`, settingActivity, activity); err != nil {
		return err
	}
	return tx.Commit()
}
