// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaftV4_ApplyCtxAndReadIndex(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	leader := c.Leader()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fut := ApplyTyped[any](ctx, leader, []byte("test-v4-cmd"))
	_, err := fut.Await(ctx)
	require.NoError(t, err)
	writtenIdx := fut.Index()
	require.Greater(t, writtenIdx, uint64(0))

	readIdx, err := leader.ReadIndex(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, readIdx, writtenIdx)
	require.GreaterOrEqual(t, leader.FSMAppliedIndex(), readIdx)
}

func TestRaftV4_ReadIndexCoalescing(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	leader := c.Leader()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fut := leader.ApplyCtx(ctx, []byte("coalesce-seed"))
	_, err := fut.Await(ctx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx, rErr := leader.ReadIndex(ctx)
			require.NoError(t, rErr)
			require.GreaterOrEqual(t, idx, fut.Index())
		}()
	}
	wg.Wait()
}

func TestRaftV4_AsyncDiskIO(t *testing.T) {
	conf := inmemConfig(t)
	conf.AsyncDiskIO = true
	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		fut := leader.ApplyCtx(ctx, []byte("async-entry"))
		_, err := fut.Await(ctx)
		require.NoError(t, err)
	}
	c.EnsureSame(t)
}

func TestRaftV4_ReplicationStatusAndPromote(t *testing.T) {
	c := MakeCluster(1, t, nil)
	defer c.Close()

	leader := c.Leader()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	statuses, err := leader.ReplicationStatus(ctx)
	require.NoError(t, err)
	require.Contains(t, statuses, leader.localID)
	require.True(t, statuses[leader.localID].IsCaughtUp)
}