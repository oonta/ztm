package policy

import "testing"

func TestPolicyAllowDeny(t *testing.T) {
	p := New().AllowService("api.internal").DenyService("admin.*")

	if !p.Allow("alice", "api.internal") {
		t.Fatal("expected allow api.internal")
	}
	if p.Allow("alice", "admin.panel") {
		t.Fatal("expected deny admin.*")
	}
	if p.Allow("alice", "db.internal") {
		t.Fatal("expected default deny")
	}
}

func TestPermissive(t *testing.T) {
	p := Permissive()
	if !p.Allow("x", "anything") {
		t.Fatal("expected permissive allow")
	}
}
