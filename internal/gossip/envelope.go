package gossip

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	ztmv1 "ztm/api/proto/ztm/v1"
	"ztm/internal/identity"
	"ztm/internal/registry"
)

// Signer signs gossip registry payloads.
type Signer interface {
	NodeID() string
	PublicKeyBase64() string
	Sign(payload []byte) []byte
}

func signEnvelope(cluster string, payload []byte, signer Signer) ([]byte, error) {
	if signer == nil {
		return nil, fmt.Errorf("gossip signer is required")
	}
	if cluster == "" {
		return nil, fmt.Errorf("cluster is required")
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload")
	}
	var state registry.LocalState
	if err := json.Unmarshal(payload, &state); err != nil {
		return nil, fmt.Errorf("payload is not registry state: %w", err)
	}
	if state.NodeID == "" {
		return nil, fmt.Errorf("registry state missing node_id")
	}
	if state.NodeID != signer.NodeID() {
		return nil, fmt.Errorf("registry state node_id mismatch")
	}

	env := &ztmv1.GossipEnvelope{
		Cluster:   cluster,
		Type:      ztmv1.GossipType_GOSSIP_TYPE_SERVICE_ANNOUNCE,
		Payload:   payload,
		Signature: signer.Sign(payload),
	}
	return proto.Marshal(env)
}

func unwrapEnvelope(cluster string, data []byte, pubKey func(nodeID string) (ed25519.PublicKey, bool)) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty gossip message")
	}
	if data[0] == '{' {
		return nil, fmt.Errorf("unsigned legacy gossip state rejected")
	}

	var env ztmv1.GossipEnvelope
	if err := proto.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("invalid gossip envelope: %w", err)
	}
	if env.GetCluster() != cluster {
		return nil, fmt.Errorf("gossip cluster mismatch")
	}
	if env.GetType() != ztmv1.GossipType_GOSSIP_TYPE_SERVICE_ANNOUNCE {
		return nil, fmt.Errorf("unsupported gossip type")
	}
	if len(env.GetPayload()) == 0 {
		return nil, fmt.Errorf("empty gossip payload")
	}
	if len(env.GetSignature()) == 0 {
		return nil, fmt.Errorf("missing gossip signature")
	}

	var state registry.LocalState
	if err := json.Unmarshal(env.GetPayload(), &state); err != nil {
		return nil, fmt.Errorf("invalid registry payload: %w", err)
	}
	if state.NodeID == "" {
		return nil, fmt.Errorf("registry state missing node_id")
	}

	pub, ok := pubKey(state.NodeID)
	if !ok {
		return nil, fmt.Errorf("unknown gossip public key for %s", state.NodeID)
	}
	if !identity.VerifyGossipSignature(pub, env.GetPayload(), env.GetSignature()) {
		return nil, fmt.Errorf("invalid gossip signature from %s", state.NodeID)
	}
	return env.GetPayload(), nil
}
