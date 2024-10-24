package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      sync.Cond
	peers     []*ClientEnd // RPC end points of all peers
	persister *Persister   // Object to hold this peer's persisted state
	applyCh   chan ApplyMsg
	me        int          // this peer's index into peers[]
	dead      atomic.Int32 // set by Kill()

	// Custom fields not explicitly mentioned in the paper
	electionTimeout time.Duration
	State           State
	LastHeartbeat   time.Time

	// Persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int

	// Volatile state on all leaders
	NextIndex  []int
	MatchIndex []int
}

func (rf *Raft) GetState() (currentTerm int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.State == Leader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var raftLog []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&raftLog) != nil {
		log.Fatalf("%d unable to decode data", rf.me)
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = raftLog
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) UpdateCommitIndex() {
	for n := rf.CommitIndex + 1; n < len(rf.Log); n++ {
		if rf.Log[n].Term == rf.CurrentTerm {
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.MatchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.CommitIndex = n
			} else {
				break
			}
		}
	}
}

func (rf *Raft) SendAppendEntries(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader {
		return
	}

	nextIndex := rf.NextIndex[peer]
	prevLogIndex := nextIndex - 1

	if prevLogIndex < 0 || prevLogIndex >= len(rf.Log) {
		return
	}

	prevLogTerm := rf.Log[prevLogIndex].Term
	entries := rf.Log[nextIndex:]

	req := AppendEntriesRequest{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.CommitIndex,
	}

	rf.mu.Unlock()

	var res AppendEntriesResponse
	// startTime := time.Now()
	ok := rf.peers[peer].Call("Raft.AppendEntries", &req, &res)
	// log.Printf("%d took %v seconds to hear back from peer %d (AE)\n", rf.me, time.Since(startTime).Seconds(), peer)

	rf.mu.Lock()

	if ok {
		if res.Success {
			rf.NextIndex[peer] = prevLogIndex + len(entries) + 1
			rf.MatchIndex[peer] = prevLogIndex + len(entries)
			rf.UpdateCommitIndex()
			rf.cond.Broadcast()
		} else if res.Term > rf.CurrentTerm {
			rf.BecomeFollower(res.Term)
		} else {
			if res.XTerm < 0 {
				rf.NextIndex[peer] = res.XLen
			} else {
				lastIndexOfXTerm := -1
				for i := len(rf.Log) - 1; i >= 0; i-- {
					if rf.Log[i].Term == res.XTerm {
						lastIndexOfXTerm = i
						break
					}
				}

				if lastIndexOfXTerm != -1 {
					rf.NextIndex[peer] = lastIndexOfXTerm + 1
				} else {
					rf.NextIndex[peer] = res.XIndex
				}
			}
			go rf.SendAppendEntries(peer)
		}
	}
}

func (rf *Raft) BroadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader {
		return
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.SendAppendEntries(peer)
		}
	}
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	res.Term = rf.CurrentTerm
	res.Success = false
	res.XTerm = -1
	res.XIndex = -1
	res.XLen = -1

	// Rule 1
	if req.Term < rf.CurrentTerm {
		return
	}

	rf.LastHeartbeat = time.Now()

	// Don't use becomeFollower() here because we might not want to update VotedFor
	if req.Term > rf.CurrentTerm {
		rf.CurrentTerm = req.Term
		rf.VotedFor = -1

		rf.persist()
	}
	rf.State = Follower

	// Rule 2
	if req.PrevLogIndex >= len(rf.Log) {
		res.XLen = len(rf.Log)
		return
	}

	if rf.Log[req.PrevLogIndex].Term != req.PrevLogTerm {
		res.XTerm = rf.Log[req.PrevLogIndex].Term

		for i := req.PrevLogIndex; i >= 0; i-- {
			if rf.Log[i].Term != res.XTerm {
				res.XIndex = i + 1
				return
			}
		}
	}
	res.Success = true

	// Rule 3
	i, j := req.PrevLogIndex+1, 0
	for i < len(rf.Log) && j < len(req.Entries) {
		if rf.Log[i].Term != req.Entries[j].Term {
			rf.Log = rf.Log[:i]
			rf.persist()
			break
		}
		i++
		j++
	}

	// Rule 4
	rf.Log = append(rf.Log, req.Entries[j:]...)

	rf.persist()

	// Rule 5
	if req.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(req.LeaderCommit, len(rf.Log)-1)
		rf.cond.Broadcast()
	}
}

func (rf *Raft) sendHeartbeat(peer int, term int) {
	rf.mu.Lock()
	if rf.State != Leader || rf.CurrentTerm != term {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	var res AppendEntriesResponse
	rf.SendAppendEntries(peer)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader || rf.CurrentTerm != term {
		return
	}

	if res.Term > rf.CurrentTerm {
		rf.BecomeFollower(res.Term)
		return
	}
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State != Leader {
			rf.mu.Unlock()
			return
		}
		currentTerm := rf.CurrentTerm
		rf.mu.Unlock()

		for i := range rf.peers {
			if i != rf.me {
				go rf.sendHeartbeat(i, currentTerm)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Section 5.4.1
// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
func (rf *Raft) isCandidateUpToDate(candidateLastIndex, candidateLastTerm int) bool {
	myLastIndex := len(rf.Log) - 1
	myLastTerm := rf.Log[myLastIndex].Term

	return candidateLastTerm > myLastTerm || (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex)
}

func (rf *Raft) RequestVote(req *RequestVoteRequest, res *RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d received %v\n", rf.me, req)

	res.Term = rf.CurrentTerm
	res.VoteGranted = false

	if req.Term < rf.CurrentTerm {
		return
	}

	if req.Term > rf.CurrentTerm {
		rf.BecomeFollower(req.Term)
	}

	if (rf.VotedFor < 0 || rf.VotedFor == req.CandidateID) && rf.isCandidateUpToDate(req.LastLogIndex, req.LastLogTerm) {
		DPrintf("%d voted for %d\n", rf.me, req.CandidateID)
		rf.VotedFor = req.CandidateID
		rf.LastHeartbeat = time.Now()
		res.VoteGranted = true

		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteRequest, reply *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) UpdateStateMachine() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.LastApplied >= rf.CommitIndex {
			rf.cond.Wait()
		}

		// Invariant: lastApplied < commitIndex
		lastApplied, commitIndex := rf.LastApplied, rf.CommitIndex
		newEntries := make([]ApplyMsg, 0, commitIndex-lastApplied)

		for i := lastApplied + 1; i <= commitIndex; i++ {
			newEntries = append(newEntries, ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Command,
				CommandIndex: i,
			})
		}

		rf.mu.Unlock()

		for _, entry := range newEntries {
			rf.applyCh <- entry
		}

		rf.mu.Lock()
		rf.LastApplied = commitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) Start(command interface{}) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index, term, isLeader = -1, -1, false

	if rf.killed() || rf.State != Leader {
		return
	}

	index = len(rf.Log)
	term = rf.CurrentTerm
	isLeader = true

	rf.Log = append(rf.Log, LogEntry{
		Command: command,
		Term:    rf.CurrentTerm,
	})

	rf.persist()
	go rf.BroadcastAppendEntries()

	return
}

func (rf *Raft) Kill() {
	rf.dead.Store(1)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load() == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	log.Printf("%d is starting an election\n", rf.me)
	startTime := time.Now()
	rf.BecomeCandidate()
	currentTerm := rf.CurrentTerm
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[len(rf.Log)-1].Term
	rf.mu.Unlock()

	votes := 1
	voteChan := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			req := RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var res RequestVoteResponse

			DPrintf("%d is requesting vote rpc to %d\n", rf.me, peer)
			if rf.sendRequestVote(peer, &req, &res) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.State != Candidate || rf.CurrentTerm != currentTerm {
					voteChan <- false
					return
				}

				if res.Term > rf.CurrentTerm {
					rf.BecomeFollower(res.Term)
					voteChan <- false
					return
				}

				voteChan <- res.VoteGranted
			} else {
				DPrintf("%d encountered error requesting vote from %d\n", rf.me, peer)
				voteChan <- false
			}
		}(i)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		if <-voteChan {
			votes++
			if votes > len(rf.peers)/2 {
				rf.mu.Lock()
				if rf.State == Candidate && rf.CurrentTerm == currentTerm {
					log.Printf("%d was elected leader", rf.me)
					DPrintf("election took %v seconds\n", time.Since(startTime).Seconds())
					rf.BecomeLeader()
				}
				rf.mu.Unlock()
				break
			}
		}
	}
}

func (rf *Raft) checkIfElectionNeeded() {
	for {
		// OK not to lock because election timeout doesn't change
		time.Sleep(rf.electionTimeout)

		rf.mu.Lock()
		isLeader := rf.State == Leader
		didTimeout := time.Since(rf.LastHeartbeat) > rf.electionTimeout
		rf.mu.Unlock()

		if !isLeader && didTimeout {
			rf.startElection()
		}
	}
}

func Make(peers []*ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,
		applyCh:   applyCh,

		electionTimeout: time.Duration(250+rand.Intn(150)) * time.Millisecond,
		State:           Follower,
		LastHeartbeat:   time.Now(),

		CurrentTerm: 0,
		VotedFor:    -1,
		Log: []LogEntry{
			{Command: -1, Term: 0},
		},

		CommitIndex: 0,
		LastApplied: 0,
	}

	rf.mu.Lock()
	rf.readPersist(rf.persister.ReadRaftState())
	rf.mu.Unlock()

	rf.cond = *sync.NewCond(&rf.mu)

	go rf.UpdateStateMachine()
	go rf.checkIfElectionNeeded()
	return rf
}
