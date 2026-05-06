package main

import (
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngestgo"
)

func TestSDKNoRetry(t *testing.T) {
	evt := inngestgo.Event{
		Name: "tests/no-retry.test",
		Data: map[string]any{
			"hi": true,
		},
		User: map[string]any{},
	}

	fnID := "test-suite-no-retry"
	test := &Test{
		ID:           fnID,
		Name:         "SDK No Retry",
		Description:  ``,
		EventTrigger: evt,
		Timeout:      45 * time.Second,
	}

	test.SetAssertions(
		test.SetRequestEvent(evt),
		test.SendTrigger(),

		test.Printf("Expecting StepFailed opcode"),

		test.ExpectRequest("Initial request", "step", 5*time.Second),
		test.ExpectGeneratorResponse([]state.GeneratorOpcode{{
			Op:          enums.OpcodeStepFailed,
			ID:          "98bf98df193bcce7c33e6bc50927cf2ac21206cb",
			Name:        "first step",
			DisplayName: inngestgo.StrPtr(`first step`),
			Error: &state.UserError{
				Name:    "Error",
				Message: "no retry plz",
			},
			Data: []byte(`{"__serialized":true,"name":"Error","message":"no retry plz","stack":""}`),
			Opts: map[string]any{},
			Userland: &struct {
				ID    string `json:"id"`
				Index int    `json:"index,omitempty"`
			}{ID: "first step"},
		}}),
		// In v4, OpcodeStepFailed is terminal — the server marks the run
		// as failed and does not invoke the function again. The try/catch
		// in the TS function does not get a chance to run.
	)

	run(t, test)
}
