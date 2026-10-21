package bridge

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

// DecodeBody accepts the public Tresor Payload representation. The bridge plugin
// may already decode UTF-8 JSON; supporting both keeps the service wire explicit.
func DecodeBody(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) > 1<<20 {
		return nil, errors.New("body exceeds limit")
	}
	var wrapper struct {
		Encoding string `json:"encoding"`
		Data     string `json:"data"`
	}
	if json.Unmarshal(raw, &wrapper) != nil {
		return nil, errors.New("invalid body")
	}
	if wrapper.Encoding == "" {
		if !json.Valid(raw) {
			return nil, errors.New("invalid body")
		}
		return raw, nil
	}
	var b []byte
	switch wrapper.Encoding {
	case "utf8":
		b = []byte(wrapper.Data)
	case "base64":
		var err error
		b, err = base64.StdEncoding.DecodeString(wrapper.Data)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported body encoding")
	}
	if len(b) > 1<<20 || !json.Valid(b) {
		return nil, errors.New("invalid body")
	}
	return b, nil
}
