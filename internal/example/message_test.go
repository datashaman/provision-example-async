package example

import "testing"

func TestNewMessageHasStableIdentity(t *testing.T) {
	first, err := NewMessage("revision-a", "schedule/example/2026-09-25T10:00:00Z", 2, BehaviorProcess)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewMessage("revision-a", "schedule/example/2026-09-25T10:00:00Z", 2, BehaviorProcess)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("message IDs differ: %q != %q", first.ID, second.ID)
	}
	if first.ID == "" || first.InvocationID == "" || first.Revision != "revision-a" || first.Sequence != 2 {
		t.Fatalf("unexpected message: %#v", first)
	}
	if first.SchemaVersion != MessageSchemaVersion {
		t.Fatalf("schema version = %q; want %q", first.SchemaVersion, MessageSchemaVersion)
	}
}

func TestNewMessageIdentityChangesOnlyWhenIdentityInputsChange(t *testing.T) {
	base, err := NewMessage("revision-a", "invocation-a", 1, BehaviorProcess)
	if err != nil {
		t.Fatal(err)
	}
	changedRevision, _ := NewMessage("revision-b", "invocation-a", 1, BehaviorProcess)
	changedInvocation, _ := NewMessage("revision-a", "invocation-b", 1, BehaviorProcess)
	changedSequence, _ := NewMessage("revision-a", "invocation-a", 2, BehaviorProcess)
	changedBehavior, _ := NewMessage("revision-a", "invocation-a", 1, BehaviorHold)

	for name, got := range map[string]Message{
		"revision":   changedRevision,
		"invocation": changedInvocation,
		"sequence":   changedSequence,
	} {
		if got.ID == base.ID {
			t.Fatalf("changing %s did not change message ID", name)
		}
	}
	if changedBehavior.ID != base.ID {
		t.Fatalf("behavior changed stable identity: %q != %q", changedBehavior.ID, base.ID)
	}
}

func TestNewMessageRejectsInvalidInputs(t *testing.T) {
	for name, tc := range map[string]struct {
		revision, invocation string
		sequence             int
		behavior             Behavior
	}{
		"revision":   {"", "invocation", 1, BehaviorProcess},
		"invocation": {"revision", "", 1, BehaviorProcess},
		"sequence":   {"revision", "invocation", 0, BehaviorProcess},
		"behavior":   {"revision", "invocation", 1, Behavior("unknown")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewMessage(tc.revision, tc.invocation, tc.sequence, tc.behavior); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
