package store

// Stream settings control response content shaping for streaming chats.
// Absent rows mean the defaults: heartbeats on, intermediate turns hidden.
const (
	settingHeartbeat        = "stream_heartbeat"
	settingShowIntermediate = "stream_show_intermediate"
	defaultHeartbeat        = true
	defaultShowIntermediate = false
)

// StreamSettings returns the service-level streaming content options.
func (s *Store) StreamSettings() (heartbeat, showIntermediate bool, err error) {
	heartbeat, showIntermediate = defaultHeartbeat, defaultShowIntermediate
	rows, err := s.db.Query(`SELECT key, value FROM settings WHERE key IN(?, ?)`, settingHeartbeat, settingShowIntermediate)
	if err != nil {
		return false, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var value int
		if err = rows.Scan(&key, &value); err != nil {
			return false, false, err
		}
		switch key {
		case settingHeartbeat:
			heartbeat = value != 0
		case settingShowIntermediate:
			showIntermediate = value != 0
		}
	}
	if err = rows.Err(); err != nil {
		return false, false, err
	}
	return heartbeat, showIntermediate, nil
}

// SetStreamSettings stores the service-level streaming content options.
func (s *Store) SetStreamSettings(heartbeat, showIntermediate bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for key, value := range map[string]bool{settingHeartbeat: heartbeat, settingShowIntermediate: showIntermediate} {
		v := 0
		if value {
			v = 1
		}
		if _, err = tx.Exec(`INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}
