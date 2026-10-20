package store

import "encoding/json"

// MarshalJSON keeps persisted JSON fields as JSON on the owner wire, not base64.
func (t Task) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID              string          `json:"id"`
		BotID           string          `json:"bot_id"`
		State           string          `json:"state"`
		Input           json.RawMessage `json:"input"`
		Snapshot        json.RawMessage `json:"snapshot"`
		Result          json.RawMessage `json:"result"`
		AssignmentID    string          `json:"assignment_id"`
		Epoch           int64           `json:"epoch"`
		CancelRequested bool            `json:"cancel_requested"`
		ParentID        string          `json:"parent_id"`
		RootID          string          `json:"root_id"`
		QueueOrder      int64           `json:"queue_order"`
	}{t.ID, t.BotID, t.State, json.RawMessage(t.Input), json.RawMessage(t.Snapshot), json.RawMessage(t.Result), t.AssignmentID, t.Epoch, t.CancelRequested, t.ParentID, t.RootID, t.QueueOrder})
}
