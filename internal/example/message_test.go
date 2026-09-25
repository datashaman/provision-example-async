package example

import "testing"

const (
	testApplicationRevision ApplicationRevision = "application-revision-a"
	testTaskArtifact        ArtifactDigest      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testInvocation          InvocationID        = "schedule/example/2026-09-25T10:00:00Z"
)

func TestNewMessageHasStableIdentity(t *testing.T) {
	first, err := NewMessage(testApplicationRevision, testTaskArtifact, testInvocation, 2, BehaviorProcess)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewMessage(testApplicationRevision, testTaskArtifact, testInvocation, 2, BehaviorProcess)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("message IDs differ: %q != %q", first.ID, second.ID)
	}
	if first.ID == "" || first.InvocationID == "" || first.ApplicationRevision != testApplicationRevision || first.TaskArtifactDigest != testTaskArtifact || first.Sequence != 2 {
		t.Fatalf("unexpected message: %#v", first)
	}
	if first.SchemaVersion != MessageSchemaVersion {
		t.Fatalf("schema version = %q; want %q", first.SchemaVersion, MessageSchemaVersion)
	}
}

func TestNewMessageIdentityChangesWhenIdentityInputsChange(t *testing.T) {
	base, err := NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, BehaviorProcess)
	if err != nil {
		t.Fatal(err)
	}
	changedApplication, _ := NewMessage("application-revision-b", testTaskArtifact, "invocation-a", 1, BehaviorProcess)
	changedArtifact, _ := NewMessage("application-revision-a", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "invocation-a", 1, BehaviorProcess)
	changedInvocation, _ := NewMessage(testApplicationRevision, testTaskArtifact, "invocation-b", 1, BehaviorProcess)
	changedSequence, _ := NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 2, BehaviorProcess)
	changedBehavior, _ := NewMessage(testApplicationRevision, testTaskArtifact, "invocation-a", 1, BehaviorHold)

	for name, got := range map[string]Message{
		"application revision": changedApplication,
		"task artifact":        changedArtifact,
		"invocation":           changedInvocation,
		"sequence":             changedSequence,
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
		application ApplicationRevision
		artifact    ArtifactDigest
		invocation  InvocationID
		sequence    int
		behavior    Behavior
	}{
		"application revision": {"", testTaskArtifact, "invocation", 1, BehaviorProcess},
		"artifact digest":      {testApplicationRevision, "not-a-digest", "invocation", 1, BehaviorProcess},
		"invocation":           {testApplicationRevision, testTaskArtifact, "", 1, BehaviorProcess},
		"sequence":             {testApplicationRevision, testTaskArtifact, "invocation", 0, BehaviorProcess},
		"behavior":             {testApplicationRevision, testTaskArtifact, "invocation", 1, Behavior("unknown")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewMessage(tc.application, tc.artifact, tc.invocation, tc.sequence, tc.behavior); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestBehaviorSemanticsAreCentralized(t *testing.T) {
	for behavior, want := range map[Behavior]BehaviorSemantics{
		BehaviorProcess:  {},
		BehaviorHold:     {Hold: true},
		BehaviorFailOnce: {FailuresBeforeSuccess: 1},
		BehaviorReject:   {Reject: true},
	} {
		got, err := behavior.Semantics()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s semantics = %#v; want %#v", behavior, got, want)
		}
	}
}
