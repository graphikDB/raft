package raft

import (
	"crypto/sha1"
	"encoding/hex"
	"github.com/graphikDB/raft/fsm"
	"github.com/graphikDB/raft/storage"
	transport2 "github.com/graphikDB/raft/transport"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"net"
	"os"
	"path/filepath"
	"time"
)

type Raft struct {
	raft *raft.Raft
	opts *Options
}

func NewRaft(fsm *fsm.FSM, lis net.Listener, opts ...Opt) (*Raft, error) {
	if err := fsm.Validate(); err != nil {
		return nil, err
	}
	options := &Options{}
	for _, o := range opts {
		o(options)
	}
	options.setDefaults()
	config := raft.DefaultConfig()
	config.NoSnapshotRestoreOnStart = options.restoreSnapshotOnRestart
	config.LocalID = raft.ServerID(options.peerID)
	if options.heartbeatTimeout != 0 {
		config.HeartbeatTimeout = options.heartbeatTimeout
	}
	if options.electionTimeout != 0 {
		config.ElectionTimeout = options.electionTimeout
	}
	transport := transport2.NewNetworkTransport(lis, options.advertise, options.maxPool, options.timeout, os.Stderr)
	snapshots, err := raft.NewFileSnapshotStore(options.raftDir, options.retainSnapshots, os.Stderr)
	if err != nil {
		return nil, err
	}
	strg, err := storage.NewStorage(filepath.Join(options.raftDir, "raft.db"))
	if err != nil {
		return nil, err
	}
	ra, err := raft.NewRaft(config, fsm, strg, strg, snapshots, transport)
	if err != nil {
		return nil, err
	}
	if options.isLeader {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}
	return &Raft{
		opts: options,
		raft: ra,
	}, nil
}

func (r *Raft) State() raft.RaftState {
	return r.raft.State()
}

func (s *Raft) Servers() ([]raft.Server, error) {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, err
	}
	return configFuture.Configuration().Servers, nil
}

func (s *Raft) Join(nodeID, addr string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "failed to parse voter %s address", nodeID)
	}
	//configFuture := s.raft.GetConfiguration()
	//if err := configFuture.Error(); err != nil {
	//	return errors.Wrap(err, "failed to get raft configuration")
	//}
	//
	//for _, srv := range configFuture.Configuration().Servers {
	//	if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
	//		// However if *both* the ID and the address are the same, then nothing -- not even
	//		// a join operation -- is needed.
	//		if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
	//			// already a member
	//			return nil
	//		}
	//
	//		future := s.raft.RemoveServer(srv.ID, 0, 0)
	//		if err := future.Error(); err != nil {
	//			return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
	//		}
	//	}
	//}
	errs := 0
	for {
		f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(tcpAddr.String()), 0, 0)
		if err := f.Error(); err != nil {
			errs++
			if errs >= 10 {
				return errors.Wrap(err, "failed to add raft voter")
			}
			time.Sleep(200 * time.Millisecond)
			continue
		} else {
			break
		}
	}
	return nil
}

func (s *Raft) LeaderAddr() string {
	return string(s.raft.Leader())
}

func (s *Raft) Stats() map[string]string {
	return s.raft.Stats()
}

func (s *Raft) Apply(bits []byte) (interface{}, error) {
	f := s.raft.Apply(bits, s.opts.timeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return nil, err
	}
	return resp, nil
}

func (r *Raft) Close() error {
	return r.raft.Shutdown().Error()
}

func (r *Raft) PeerID() string {
	return r.opts.peerID
}

func hash(val []byte) string {
	h := sha1.New()
	h.Write(val)
	bs := h.Sum(nil)
	return hex.EncodeToString(bs)
}
