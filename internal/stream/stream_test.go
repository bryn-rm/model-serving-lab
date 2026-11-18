package stream

import (
	"strings"
	"testing"

	"model-serving-lab/internal/sse"
)

func TestErrorInfoKeepsOnlyStructuredFields(t *testing.T) {
	long := strings.Repeat("x", 65)
	for name, tc := range map[string]struct {
		event sse.Event
		want  ErrorInfo
	}{
		"current":       {sse.Event{Data: `{"error":{"message":"prompt echo","type":"BadRequestError","code":400}}`}, ErrorInfo{"BadRequestError", "400"}},
		"legacy":        {sse.Event{Data: `{"object":"error","message":"prompt echo","type":"invalid_request_error","code":"bad_len"}`}, ErrorInfo{"invalid_request_error", "bad_len"}},
		"event-json":    {sse.Event{Type: "error", Data: `{"type":"overloaded","code":503}`}, ErrorInfo{"overloaded", "503"}},
		"event-text":    {sse.Event{Type: "error", Data: "prompt echo"}, ErrorInfo{}},
		"string-error":  {sse.Event{Data: `{"error":"prompt echo"}`}, ErrorInfo{}},
		"free-text":     {sse.Event{Data: `{"error":{"type":"has spaces in it","code":1.5}}`}, ErrorInfo{"unrecognised", ""}},
		"too-long-code": {sse.Event{Data: `{"error":{"code":"` + long + `"}}`}, ErrorInfo{"", "unrecognised"}},
	} {
		chunk, err := Inspect(tc.event)
		if err != nil || chunk.Error == "" || chunk.ErrorInfo != tc.want {
			t.Errorf("%s: got %+v err=%v, want %+v", name, chunk.ErrorInfo, err, tc.want)
		}
		if strings.Contains(chunk.ErrorInfo.Type+chunk.ErrorInfo.Code, "prompt") {
			t.Errorf("%s: message text leaked into ErrorInfo", name)
		}
	}
	for body, want := range map[string]ErrorInfo{
		`{"error":{"message":"m","type":"overloaded_error","code":"admission_rejected"}}`: {"overloaded_error", "admission_rejected"},
		`{"object":"error","message":"m","type":"NotFoundError","code":404}`:              {"NotFoundError", "404"},
		`not json`: {},
	} {
		if got := EnvelopeInfo([]byte(body)); got != want {
			t.Errorf("EnvelopeInfo(%s) = %+v, want %+v", body, got, want)
		}
	}
}
