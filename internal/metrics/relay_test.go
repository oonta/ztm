package metrics

import "testing"

func TestSnapshotRelayErrorRate(t *testing.T) {
	m := New()
	m.RecordRelayFailure()
	m.RecordRelaySuccess()
	m.RecordRelaySuccess()

	snap := m.SnapshotRelay()
	if snap.ErrorRate < 0.33 || snap.ErrorRate > 0.34 {
		t.Fatalf("error rate: got %v want ~0.33", snap.ErrorRate)
	}
}

func TestSnapshotRelayActiveStreams(t *testing.T) {
	m := New()
	m.TunnelStreamOpened()
	m.MeshRelayStarted()

	snap := m.SnapshotRelay()
	if snap.ActiveStreams != 2 {
		t.Fatalf("active streams: got %d want 2", snap.ActiveStreams)
	}
}
