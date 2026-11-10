# Raft v4 Architectural Summary & Usage Guide

The **ProtocolVersion 4 (`v4`)** extension modernizes `hashicorp/raft` with native `context.Context` lifecycles, Go generics, disk-I/O-free linearizable reads, asynchronous disk-replication pipelining, and active observer/nonvoter management while maintaining 100% backward compatibility with the existing `FSM` and `LogStore` interfaces.

---

## 1. Summary of Modifications

* **`api_v4.go` (New):** Implements `FutureV4[T]`, `ApplyTyped[T]`, `ApplyCtx`, `ApplyLogCtx`, `VerifyLeaderCtx`, `BarrierCtx`, coalesced `ReadIndex` (singleflight batching), `inflightLogBuffer`, `ReplicationStatus`, `PromotePeerCtx`, `tryAutoPromoteNonvoters`, and the `TelemetryHook` interface.
* **`config.go`:** Bumps `ProtocolVersionMax` from `3` to `4` and adds four optional configuration fields to `Config`: `AsyncDiskIO`, `MaxPromotionLag`, `AutoPromoteNonvoter`, and `TelemetryHook`.
* **`state.go` & `fsm.go`:** Adds atomic `fsmApplied` tracking and waiter notification channels (`registerFSMWaiter` / `setFSMApplied`) so `ReadIndex` waits until the `FSM` goroutine has actually executed the target log index rather than merely enqueuing it into `fsmMutateCh`.
* **`raft.go` & `replication.go`:** Decouples local `LogStore.StoreLogs` disk writes from follower replication when `AsyncDiskIO` is enabled. Followers read in-flight entries directly from `inflightLogBuffer` (`getLogOrInflight`) while the leader's disk flush completes concurrently on `diskIoCh`.
* **`v4_test.go` & `raft_test.go`:** Adds unit tests covering `ApplyTyped`, coalesced `ReadIndex`, `AsyncDiskIO`, and `ReplicationStatus`, and updates `TestRaft_LeaderID_Propagated` to assert `ProtocolVersionMax`.

---

## 2. Usage Examples

### Configuration (`Config`)

```go
conf := raft.DefaultConfig() // Defaults to ProtocolVersion 4
conf.LocalID = raft.ServerID("node-1")
conf.AsyncDiskIO = true          // Parallelize leader WAL sync and network replication
conf.AutoPromoteNonvoter = true  // Automatically promote caught-up Nonvoters to Voters
conf.MaxPromotionLag = 64        // Max log lag for a Nonvoter to be considered caught up
```

### Context-Aware & Type-Safe Writes (`ApplyTyped` / `ApplyCtx`)

```go
ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
defer cancel()

// Type-safe generic future expecting *MyFSMResponse from FSM.Apply()
fut := raft.ApplyTyped[*MyFSMResponse](ctx, raftNode, payload)
resp, err := fut.Await(ctx)
if err != nil {
    return err // Handles both Raft errors and ctx.DeadlineExceeded / ctx.Canceled
}
logIndex := fut.Index()
```

### Wait-Free Linearizable Reads (`ReadIndex`)

```go
// Verifies quorum leadership via a coalesced heartbeat round (zero disk I/O)
// and blocks until FSM.Apply() has caught up to the committed read index.
if _, err := raftNode.ReadIndex(ctx); err != nil {
    return nil, err
}
// Safe to read directly from local in-memory FSM state
return localFSM.GetState(key), nil
```

### Observer Monitoring & Hot-Standby Promotion (`ReplicationStatus` & `PromotePeerCtx`)

```go
statuses, err := raftNode.ReplicationStatus(ctx)
if err == nil {
    for id, st := range statuses {
        if st.Suffrage == raft.Nonvoter && st.IsCaughtUp {
            // Atomically promote Nonvoter to Voter if replication lag <= 32 entries
            _ = raftNode.PromotePeerCtx(ctx, id, 32)
        }
    }
}
```