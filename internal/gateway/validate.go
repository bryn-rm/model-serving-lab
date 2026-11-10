package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"
)

// allowedFields is the plan's explicit top-level whitelist.
var allowedFields = map[string]bool{
	"model": true, "messages": true, "stream": true, "stream_options": true,
	"max_tokens": true, "max_completion_tokens": true, "temperature": true,
	"top_p": true, "seed": true, "ignore_eos": true, "n": true,
}

var allowedRoles = map[string]bool{"system": true, "user": true, "assistant": true}

// RequestError is a client error reported before streaming starts.
type RequestError struct {
	Code, Param, Message string
}

func (e *RequestError) Error() string { return e.Message }

func invalid(code, param, format string, args ...any) *RequestError {
	return &RequestError{Code: code, Param: param, Message: fmt.Sprintf(format, args...)}
}

// ValidateRequest checks a chat-completion body against the supported
// contract. It never rewrites the body: the caller forwards the same bytes.
// Values are type-checked strictly (no null, integers must be integral) so
// the gateway and vLLM cannot interpret a field differently.
func ValidateRequest(body []byte, model string, maxOutputTokens int) *RequestError {
	if !utf8.Valid(body) {
		return invalid("invalid_json", "", "request body must be UTF-8 JSON")
	}
	if !json.Valid(body) {
		return invalid("invalid_json", "", "request body must be a JSON object")
	}
	if err := uniqueJSONKeys(body); err != nil {
		return invalid("invalid_json", "", "request body must contain unique JSON object keys")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return invalid("invalid_json", "", "request body must be a JSON object")
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if !allowedFields[k] {
			return invalid("unsupported_field", k, "unsupported field %q", k)
		}
		if bytes.Equal(bytes.TrimSpace(fields[k]), []byte("null")) {
			return invalid("invalid_value", k, "%s must not be null", k)
		}
	}
	for _, k := range []string{"model", "messages", "stream"} {
		if _, ok := fields[k]; !ok {
			return invalid("missing_field", k, "%s is required", k)
		}
	}

	var m string
	if json.Unmarshal(fields["model"], &m) != nil || m != model {
		return invalid("model_not_found", "model", "model must be %q", model)
	}
	var stream bool
	if json.Unmarshal(fields["stream"], &stream) != nil || !stream {
		return invalid("invalid_value", "stream", "stream must be true")
	}
	if raw, ok := fields["n"]; ok {
		if n, ok := integer(raw); !ok || n != 1 {
			return invalid("invalid_value", "n", "n must be 1")
		}
	}
	if err := validateMessages(fields["messages"]); err != nil {
		return err
	}

	_, hasMax := fields["max_tokens"]
	_, hasMaxCompletion := fields["max_completion_tokens"]
	switch {
	case hasMax && hasMaxCompletion:
		return invalid("invalid_value", "max_tokens", "send only one of max_tokens and max_completion_tokens")
	case !hasMax && !hasMaxCompletion:
		return invalid("missing_field", "max_tokens", "max_tokens or max_completion_tokens is required")
	}
	limitField := "max_tokens"
	if hasMaxCompletion {
		limitField = "max_completion_tokens"
	}
	if n, ok := integer(fields[limitField]); !ok || n < 1 || n > int64(maxOutputTokens) {
		return invalid("invalid_value", limitField, "%s must be an integer from 1 to %d", limitField, maxOutputTokens)
	}

	if raw, ok := fields["temperature"]; ok {
		if v, ok := number(raw); !ok || v < 0 || v > 2 {
			return invalid("invalid_value", "temperature", "temperature must be a number from 0 to 2")
		}
	}
	if raw, ok := fields["top_p"]; ok {
		if v, ok := number(raw); !ok || v <= 0 || v > 1 {
			return invalid("invalid_value", "top_p", "top_p must be a number in (0, 1]")
		}
	}
	if raw, ok := fields["seed"]; ok {
		if _, ok := integer(raw); !ok {
			return invalid("invalid_value", "seed", "seed must be a 64-bit integer")
		}
	}
	if raw, ok := fields["ignore_eos"]; ok {
		var b bool
		if json.Unmarshal(raw, &b) != nil {
			return invalid("invalid_value", "ignore_eos", "ignore_eos must be a boolean")
		}
	}
	if raw, ok := fields["stream_options"]; ok {
		var opts map[string]json.RawMessage
		if json.Unmarshal(raw, &opts) != nil {
			return invalid("invalid_value", "stream_options", "stream_options must be an object")
		}
		for k, v := range opts {
			var b bool
			if k != "include_usage" || json.Unmarshal(v, &b) != nil || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				return invalid("invalid_value", "stream_options", "stream_options supports only include_usage (boolean)")
			}
		}
	}
	return nil
}

func validateMessages(raw json.RawMessage) *RequestError {
	var messages []map[string]json.RawMessage
	if json.Unmarshal(raw, &messages) != nil || len(messages) == 0 {
		return invalid("invalid_value", "messages", "messages must be a nonempty array of objects")
	}
	for i, msg := range messages {
		param := fmt.Sprintf("messages[%d]", i)
		for k := range msg {
			if k != "role" && k != "content" {
				return invalid("unsupported_field", param, "%s: unsupported field %q", param, k)
			}
		}
		var role, content string
		if json.Unmarshal(msg["role"], &role) != nil || !allowedRoles[role] {
			return invalid("invalid_value", param, "%s: role must be system, user or assistant", param)
		}
		if json.Unmarshal(msg["content"], &content) != nil || strings.TrimSpace(content) == "" {
			return invalid("invalid_value", param, "%s: content must be nonempty text", param)
		}
	}
	return nil
}

// integer accepts only an integral JSON number that fits int64.
func integer(raw json.RawMessage) (int64, bool) {
	n, ok := jsonNumber(raw)
	if !ok {
		return 0, false
	}
	v, err := n.Int64()
	return v, err == nil
}

func number(raw json.RawMessage) (float64, bool) {
	n, ok := jsonNumber(raw)
	if !ok {
		return 0, false
	}
	v, err := n.Float64()
	return v, err == nil
}

func jsonNumber(raw json.RawMessage) (json.Number, bool) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if d.Decode(&v) != nil {
		return "", false
	}
	n, ok := v.(json.Number)
	return n, ok
}

// Reject ambiguous objects before validating and forwarding the original bytes.
// Compare decoded keys so escaped spellings cannot bypass this check.
func uniqueJSONKeys(body []byte) error {
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	var value func() error
	value = func() error {
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name := key.(string)
				if seen[name] {
					return fmt.Errorf("duplicate object key")
				}
				seen[name] = true
				if err := value(); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := value(); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected delimiter")
		}
		_, err = d.Token()
		return err
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}
