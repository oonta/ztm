package gossip

import (
	"encoding/json"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"ztm/internal/registry"
)

// NodeMeta is published in memberlist node metadata.
type NodeMeta struct {
	NodeID   string `json:"node_id"`
	Cluster  string `json:"cluster"`
	MeshAddr string `json:"mesh_addr"`
	RPCAddr  string `json:"rpc_addr"`
}

// Member describes a cluster member.
type Member struct {
	Name string   `json:"name"`
	Addr string   `json:"addr"`
	Meta NodeMeta `json:"meta"`
}

// OnMemberFunc is called when a member joins or updates metadata.
type OnMemberFunc func(meta NodeMeta)

// Delegate implements memberlist delegates for ZTM gossip.
type Delegate struct {
	nodeID   string
	cluster  string
	meshAddr string
	rpcAddr  string
	registry *registry.Registry

	onMember OnMemberFunc

	mu        sync.Mutex
	broadcasts []memberlist.Broadcast
}

func NewDelegate(nodeID, cluster, meshAddr, rpcAddr string, reg *registry.Registry, onMember OnMemberFunc) *Delegate {
	return &Delegate{
		nodeID:   nodeID,
		cluster:  cluster,
		meshAddr: meshAddr,
		rpcAddr:  rpcAddr,
		registry: reg,
		onMember: onMember,
	}
}

func (d *Delegate) NodeMeta(limit int) []byte {
	data, err := json.Marshal(NodeMeta{
		NodeID:   d.nodeID,
		Cluster:  d.cluster,
		MeshAddr: d.meshAddr,
		RPCAddr:  d.rpcAddr,
	})
	if err != nil {
		return nil
	}
	if len(data) > limit {
		return data[:limit]
	}
	return data
}

func (d *Delegate) NotifyMsg(msg []byte) {
	_ = d.registry.MergeRemoteState(msg)
}

func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.broadcasts) == 0 {
		return nil
	}
	var out [][]byte
	for len(d.broadcasts) > 0 && limit > 0 {
		b := d.broadcasts[0]
		d.broadcasts = d.broadcasts[1:]
		out = append(out, b.Message())
		limit--
	}
	return out
}

func (d *Delegate) LocalState(join bool) []byte {
	data, err := d.registry.MarshalLocalState()
	if err != nil {
		return nil
	}
	return data
}

func (d *Delegate) MergeRemoteState(buf []byte, join bool) {
	if err := d.registry.MergeRemoteState(buf); err != nil {
		log.Printf("gossip: merge remote state: %v", err)
	}
}

func (d *Delegate) NotifyJoin(node *memberlist.Node) {
	d.handleMember(node)
}

func (d *Delegate) NotifyLeave(node *memberlist.Node) {
	log.Printf("gossip: member left %s", node.Name)
}

func (d *Delegate) NotifyUpdate(node *memberlist.Node) {
	d.handleMember(node)
}

func (d *Delegate) handleMember(node *memberlist.Node) {
	if len(node.Meta) == 0 || node.Name == d.nodeID {
		return
	}
	var meta NodeMeta
	if err := json.Unmarshal(node.Meta, &meta); err != nil {
		log.Printf("gossip: bad meta from %s: %v", node.Name, err)
		return
	}
	if meta.Cluster != d.cluster || meta.MeshAddr == "" {
		return
	}
	if d.onMember != nil {
		d.onMember(meta)
	}
}

func (d *Delegate) QueueStateBroadcast() {
	state, err := d.registry.MarshalLocalState()
	if err != nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.broadcasts = append(d.broadcasts, &stateBroadcast{data: state})
}

type stateBroadcast struct {
	data []byte
}

func (b *stateBroadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *stateBroadcast) Message() []byte {
	return b.data
}

func (b *stateBroadcast) Finished() {}

// Cluster wraps hashicorp memberlist.
type Cluster struct {
	ml       *memberlist.Memberlist
	delegate *Delegate
}

// Config for gossip cluster.
type Config struct {
	NodeID    string
	Cluster   string
	MeshAddr  string
	RPCAddr   string
	BindAddr  string
	Registry  *registry.Registry
	SecretKey []byte
	OnMember  OnMemberFunc
}

// Start creates and joins the gossip cluster.
func Start(cfg Config, join []string) (*Cluster, error) {
	delegate := NewDelegate(cfg.NodeID, cfg.Cluster, cfg.MeshAddr, cfg.RPCAddr, cfg.Registry, cfg.OnMember)

	host, port, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, err
	}

	mlCfg := memberlist.DefaultLocalConfig()
	mlCfg.Name = cfg.NodeID
	mlCfg.BindAddr = host
	mlCfg.BindPort = parsePort(port)
	mlCfg.AdvertiseAddr = host
	mlCfg.AdvertisePort = mlCfg.BindPort
	mlCfg.PushPullInterval = 3 * time.Second
	mlCfg.Delegate = delegate
	mlCfg.Events = delegate
	if len(cfg.SecretKey) > 0 {
		mlCfg.SecretKey = cfg.SecretKey
	}

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, err
	}

	if len(join) > 0 {
		if _, err := ml.Join(join); err != nil {
			_ = ml.Shutdown()
			return nil, err
		}
	}

	return &Cluster{ml: ml, delegate: delegate}, nil
}

func (c *Cluster) Addr() string {
	node := c.ml.LocalNode()
	return net.JoinHostPort(node.Addr.String(), strconv.Itoa(int(node.Port)))
}

func (c *Cluster) Members() []Member {
	nodes := c.ml.Members()
	out := make([]Member, 0, len(nodes))
	for _, n := range nodes {
		m := Member{Name: n.Name, Addr: n.Addr.String()}
		if len(n.Meta) > 0 {
			_ = json.Unmarshal(n.Meta, &m.Meta)
		}
		out = append(out, m)
	}
	return out
}

func (c *Cluster) MemberCount() int {
	return c.ml.NumMembers()
}

func (c *Cluster) BroadcastState() {
	c.delegate.QueueStateBroadcast()
}

// PropagateState pushes registry state to all members immediately.
func (c *Cluster) PropagateState(data []byte) {
	if len(data) == 0 {
		return
	}
	local := c.ml.LocalNode().Name
	for _, n := range c.ml.Members() {
		if n.Name == local {
			continue
		}
		_ = c.ml.SendBestEffort(n, data)
	}
}

func (c *Cluster) Shutdown() error {
	return c.ml.Shutdown()
}

func parsePort(port string) int {
	p, err := strconv.Atoi(port)
	if err != nil {
		return 0
	}
	return p
}
