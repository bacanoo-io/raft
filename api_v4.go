// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/hashicorp/go-metrics"
)

var (
	ErrPeerNotCaughtUp = errors.New("peer is not caught up to leader log")
	ErrPeerNotFound    = errors.New("peer not found in cluster configuration")
)

type TelemetryHook interface {
	OnApply(count int, duration time.Duration)
	OnCommit(index uint64, latency time.Duration)
	OnReadIndex(batchSize int, latency time.Duration)
	OnDiskSync(count int, duration time.Duration, err error)
	OnPeerPromoted(peerID ServerID)
}

type FutureV4[T any] interface {
	Await(ctx context.Context) (T, error)
	Index() uint64
}

type genericApplyFuture[T any] struct {
	logFuture *logFuture
	err       error
}

func (g *genericApplyFuture[T]) Await(ctx context.Context) (T, error) {
	var zero T
	if g.err != nil {
		return zero, g.err
	}
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-g.logFuture.ShutdownCh:
		return zero, ErrRaftShutdown
	case err := <-g.logFuture.errCh:
		g.logFuture.err = err
		if err != nil {
			return zero, err
		}
		if g.logFuture.response == nil {
			return zero, nil
		}
		typed, ok := g.logFuture.response.(T)
		if !ok {
			return zero, fmt.Errorf("unexpected FSM response type %T", g.logFuture.response)
		}
		return typed, nil
	}
}

func (g *genericApplyFuture[T]) Index() uint64 {
	if g.logFuture == nil {
		return 0
	}
	return g.logFuture.Index()
}

func ApplyTyped[T any](ctx context.Context, r *Raft, cmd []byte) FutureV4[T] {
	metrics.IncrCounter([]string{"raft", "apply_v4"}, 1)

	lf := &logFuture{
		log: Log{
			Type: LogCommand,
			Data: cmd,
		},
	}
	lf.init()
	lf.ShutdownCh = r.shutdownCh

	select {
	case <-ctx.Done():
		return &genericApplyFuture[T]{err: ctx.Err()}
	case <-r.shutdownCh:
		return &genericApplyFuture[T]{err: ErrRaftShutdown}
	case r.applyCh <- lf:
		return &genericApplyFuture[T]{logFuture: lf}
	}
}

func (r *Raft) ApplyCtx(ctx context.Context, cmd []byte) FutureV4[any] {
	return ApplyTyped[any](ctx, r, cmd)
}

func (r *Raft) ApplyLogCtx(ctx context.Context, log Log) FutureV4[any] {
	metrics.IncrCounter([]string{"raft", "apply_v4"}, 1)

	lf := &logFuture{
		log: Log{
			Type:       LogCommand,
			Data:       log.Data,
			Extensions: log.Extensions,
		},
	}
	lf.init()
	lf.ShutdownCh = r.shutdownCh

	select {
	case <-ctx.Done():
		return &genericApplyFuture[any]{err: ctx.Err()}
	case <-r.shutdownCh:
		return &genericApplyFuture[any]{err: ErrRaftShutdown}
	case r.applyCh <- lf:
		return &genericApplyFuture[any]{logFuture: lf}
	}
}

func (r *Raft) VerifyLeaderCtx(ctx context.Context) error {
	metrics.IncrCounter([]string{"raft", "verify_leader_v4"}, 1)
	vf := &verifyFuture{}
	vf.init()
	vf.ShutdownCh = r.shutdownCh

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.verifyCh <- vf:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case err := <-vf.errCh:
		return err
	}
}

func (r *Raft) BarrierCtx(ctx context.Context) error {
	metrics.IncrCounter([]string{"raft", "barrier_v4"}, 1)
	lf := &logFuture{log: Log{Type: LogBarrier}}
	lf.init()
	lf.ShutdownCh = r.shutdownCh

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.applyCh <- lf:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case err := <-lf.errCh:
		return err
	}
}

func (r *Raft) FSMAppliedIndex() uint64 {
	return r.getFSMApplied()
}

type readIndexBatch struct {
	doneCh  chan struct{}
	readIdx uint64
	err     error
	waiters int
}

func (r *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	metrics.IncrCounter([]string{"raft", "read_index"}, 1)
	start := time.Now()
	defer metrics.MeasureSince([]string{"raft", "read_index", "latency"}, start)

	if r.getState() != Leader {
		return 0, ErrNotLeader
	}

	if err := r.waitForTermCommit(ctx); err != nil {
		return 0, err
	}

	r.readBatchMu.Lock()
	batch := r.activeReadBatch
	isLeaderCaller := false
	if batch == nil {
		batch = &readIndexBatch{
			doneCh:  make(chan struct{}),
			readIdx: r.getCommitIndex(),
		}
		r.activeReadBatch = batch
		isLeaderCaller = true
	}
	batch.waiters++
	r.readBatchMu.Unlock()

	if isLeaderCaller {
		verifyCtx, cancel := context.WithTimeout(context.Background(), r.config().HeartbeatTimeout)
		err := r.VerifyLeaderCtx(verifyCtx)
		cancel()

		r.readBatchMu.Lock()
		batch.err = err
		if c := r.config(); c.TelemetryHook != nil {
			c.TelemetryHook.OnReadIndex(batch.waiters, time.Since(start))
		}
		r.activeReadBatch = nil
		close(batch.doneCh)
		r.readBatchMu.Unlock()
	} else {
		select {
		case <-batch.doneCh:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-r.shutdownCh:
			return 0, ErrRaftShutdown
		}
	}

	if batch.err != nil {
		return 0, batch.err
	}

	if err := r.waitForFSMApplied(ctx, batch.readIdx); err != nil {
		return 0, err
	}

	return batch.readIdx, nil
}

func (r *Raft) waitForTermCommit(ctx context.Context) error {
	for {
		if r.getState() != Leader {
			return ErrNotLeader
		}
		_, lastTerm := r.getLastEntry()
		if r.getCommitIndex() > 0 && lastTerm == r.getCurrentTerm() && r.getFSMApplied() > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.shutdownCh:
			return ErrRaftShutdown
		case <-time.After(1 * time.Millisecond):
		}
	}
}

func (r *Raft) waitForFSMApplied(ctx context.Context, targetIndex uint64) error {
	if r.getFSMApplied() >= targetIndex {
		return nil
	}

	waiter := make(chan struct{}, 1)
	r.registerFSMWaiter(targetIndex, waiter)
	defer r.unregisterFSMWaiter(targetIndex, waiter)

	if r.getFSMApplied() >= targetIndex {
		return nil
	}

	select {
	case <-waiter:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCh:
		return ErrRaftShutdown
	}
}

type asyncDiskResult struct {
	lastIndex uint64
	term      uint64
	applyLogs []*logFuture
	err       error
}

type inflightLogBuffer struct {
	mu   sync.RWMutex
	logs map[uint64]*Log
}

func newInflightLogBuffer() *inflightLogBuffer {
	return &inflightLogBuffer{
		logs: make(map[uint64]*Log, 128),
	}
}

func (b *inflightLogBuffer) put(entries []*Log) {
	b.mu.Lock()
	for _, l := range entries {
		b.logs[l.Index] = l
	}
	b.mu.Unlock()
}

func (b *inflightLogBuffer) get(index uint64, out *Log) bool {
	b.mu.RLock()
	l, ok := b.logs[index]
	if ok {
		*out = *l
	}
	b.mu.RUnlock()
	return ok
}

func (b *inflightLogBuffer) deleteUpTo(maxIndex uint64) {
	b.mu.Lock()
	for idx := range b.logs {
		if idx <= maxIndex {
			delete(b.logs, idx)
		}
	}
	b.mu.Unlock()
}

func (r *Raft) getLogOrInflight(index uint64, out *Log) error {
	if r.inflightBuf != nil && r.inflightBuf.get(index, out) {
		return nil
	}
	return r.logs.GetLog(index, out)
}

type PeerReplicationStatus struct {
	ID             ServerID
	Address        ServerAddress
	Suffrage       ServerSuffrage
	MatchIndex     uint64
	NextIndex      uint64
	ReplicationLag uint64
	LastContact    time.Time
	Failures       uint64
	IsCaughtUp     bool
}

type statusFuture struct {
	deferError
	peers map[ServerID]PeerReplicationStatus
}

func (r *Raft) ReplicationStatus(ctx context.Context) (map[ServerID]PeerReplicationStatus, error) {
	if r.getState() != Leader {
		return nil, ErrNotLeader
	}

	sf := &statusFuture{}
	sf.init()
	sf.ShutdownCh = r.shutdownCh

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.shutdownCh:
		return nil, ErrRaftShutdown
	case r.statusCh <- sf:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.shutdownCh:
		return nil, ErrRaftShutdown
	case err := <-sf.errCh:
		if err != nil {
			return nil, err
		}
		return sf.peers, nil
	}
}

func (r *Raft) collectReplicationStatus() map[ServerID]PeerReplicationStatus {
	lastIdx := r.getLastIndex()
	leaseTimeout := r.config().LeaderLeaseTimeout
	maxLag := r.config().MaxPromotionLag
	if maxLag == 0 {
		maxLag = uint64(r.config().MaxAppendEntries)
	}

	result := make(map[ServerID]PeerReplicationStatus, len(r.configurations.latest.Servers))
	result[r.localID] = PeerReplicationStatus{
		ID:             r.localID,
		Address:        r.localAddr,
		Suffrage:       Voter,
		MatchIndex:     lastIdx,
		NextIndex:      lastIdx + 1,
		ReplicationLag: 0,
		LastContact:    time.Now(),
		Failures:       0,
		IsCaughtUp:     true,
	}

	for _, srv := range r.configurations.latest.Servers {
		if srv.ID == r.localID {
			continue
		}
		repl, ok := r.leaderState.replState[srv.ID]
		if !ok {
			result[srv.ID] = PeerReplicationStatus{
				ID:             srv.ID,
				Address:        srv.Address,
				Suffrage:       srv.Suffrage,
				ReplicationLag: lastIdx,
				IsCaughtUp:     false,
			}
			continue
		}

		nextIdx := atomic.LoadUint64(&repl.nextIndex)
		var matchIdx uint64
		if nextIdx > 0 {
			matchIdx = nextIdx - 1
		}
		var lag uint64
		if lastIdx > matchIdx {
			lag = lastIdx - matchIdx
		}
		lastContact := repl.LastContact()
		failures := atomic.LoadUint64(&repl.failures)
		isCaughtUp := lag <= maxLag && time.Since(lastContact) <= leaseTimeout && failures == 0

		result[srv.ID] = PeerReplicationStatus{
			ID:             srv.ID,
			Address:        srv.Address,
			Suffrage:       srv.Suffrage,
			MatchIndex:     matchIdx,
			NextIndex:      nextIdx,
			ReplicationLag: lag,
			LastContact:    lastContact,
			Failures:       failures,
			IsCaughtUp:     isCaughtUp,
		}
	}
	return result
}

func (r *Raft) PromotePeerCtx(ctx context.Context, id ServerID, maxLag uint64) error {
	if r.protocolVersion < 3 {
		return ErrUnsupportedProtocol
	}

	statuses, err := r.ReplicationStatus(ctx)
	if err != nil {
		return err
	}

	st, exists := statuses[id]
	if !exists {
		return ErrPeerNotFound
	}
	if st.Suffrage == Voter {
		return nil
	}
	if maxLag > 0 && st.ReplicationLag > maxLag {
		return fmt.Errorf("%w: current lag is %d (max allowed %d)", ErrPeerNotCaughtUp, st.ReplicationLag, maxLag)
	}
	if maxLag == 0 && !st.IsCaughtUp {
		return fmt.Errorf("%w: lag=%d, lastContact=%v", ErrPeerNotCaughtUp, st.ReplicationLag, time.Since(st.LastContact))
	}

	var timeout time.Duration
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
		if timeout <= 0 {
			return context.DeadlineExceeded
		}
	}

	fut := r.AddVoter(id, st.Address, 0, timeout)
	done := make(chan error, 1)
	go func() {
		done <- fut.Error()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err == nil {
			if hook := r.config().TelemetryHook; hook != nil {
				hook.OnPeerPromoted(id)
			}
		}
		return err
	}
}

func (r *Raft) tryAutoPromoteNonvoters() {
	if !r.config().AutoPromoteNonvoter || r.configurationChangeChIfStable() == nil {
		return
	}
	statuses := r.collectReplicationStatus()
	for _, st := range statuses {
		if st.Suffrage == Nonvoter && st.IsCaughtUp {
			r.logger.Info("auto-promoting caught-up nonvoter to voter", "peer", st.ID, "lag", st.ReplicationLag)
			fut := &configurationChangeFuture{
				req: configurationChangeRequest{
					command:       AddVoter,
					serverID:      st.ID,
					serverAddress: st.Address,
				},
			}
			fut.init()
			r.appendConfigurationEntry(fut)
			if hook := r.config().TelemetryHook; hook != nil {
				hook.OnPeerPromoted(st.ID)
			}
			return
		}
	}
}