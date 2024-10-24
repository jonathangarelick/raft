# Raft implementation in Go

This repository contains my implementation of Raft. It follows the spec detailed in the paper, with an improved log synchronization algorithm.

File structure:
- `raft.go`: main implementation
- `rpc.go`: RPC signatures (AppendEntries, RequestVote)
- `state.go`: state enum (Leader, Follower, Candidate) and state transition functions
- `persister.go`, `simrpc.go`, `util.go` boilerplate provided by MIT - thank you!

For any questions, please file an issue or email the author at `jonathan [at] garelick [dot] net`.
