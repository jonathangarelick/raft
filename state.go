package raft

import "time"

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (rf *Raft) BecomeFollower(term int) {
	defer rf.persist()

	rf.State = Follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.LastHeartbeat = time.Now()
}

func (rf *Raft) BecomeCandidate() {
	defer rf.persist()

	rf.State = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.LastHeartbeat = time.Now()
}

func (rf *Raft) BecomeLeader() {
	defer rf.persist()

	rf.State = Leader
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchIndex[i] = -1
	}

	go rf.sendHeartbeats()
}
