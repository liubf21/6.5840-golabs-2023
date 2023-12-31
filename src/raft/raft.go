package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int        // lastest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidatedId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialzied to 0...)

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index+1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increased monotonically)

	state         State
	lastTimeHeard time.Time // last time we heard from leader (used to detect leader failure)

	applyMsgCh chan ApplyMsg
}

type State uint8

const (
	Followers State = iota
	Candidates
	Leaders
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leaders {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	Debugf(dVote, "%v get requestVote from %v in term %v", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.lastTimeHeard = time.Now()
	// Reply false if term < currentTerm
	if args.Term < reply.Term {
		reply.VoteGranted = false
		return
	}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if args.Term > reply.Term { // votedFor is null in new term
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = Followers
		Debugf(dVote, "%v voted for %v in term %v", rf.me, args.CandidateId, args.Term)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contain entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debugf(dClient, "%v try to Append %v in term %v", rf.me, args.Entries, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.state = Followers
	rf.lastTimeHeard = time.Now()
	if args.Term < reply.Term { // Reply false if term < currentTerm
		reply.Success = false
		Debugf(dClient, "%v AppendEntries fail because args.Term %v < reply.Term %v", rf.me, args.Term, reply.Term)
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		Debugf(dClient, "%v AppendEntries fail because log not contain entry at PrevLogIndex %v whose term matches PrevLogTerm %v", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if len(rf.log) > args.PrevLogIndex+1 {
		rf.log = rf.log[:args.PrevLogIndex+1]
		Debugf(dClient, "%v AppendEntries delete log %v", rf.me, rf.log)
	}

	// Append any new entries not already in the log
	if reply.Success {
		rf.currentTerm = args.Term
		if len(args.Entries) > 0 {
			Debugf(dClient, "%v Append %v \n", rf.me, args.Entries)
			for _, e := range args.Entries {
				rf.log = append(rf.log, e)
				rf.applyMsgCh <- ApplyMsg{
					CommandValid: true,
					Command:      e.Command,
					CommandIndex: len(rf.log) - 1,
				}
			}
		}
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.commitIndex + 1

	if isLeader { // start agreement on the next command to be appended to Raft's log
		Debugf(dLeader, "starting agreement on command %v, commitIndex: %v, lastApplied: %v", command, rf.commitIndex, rf.lastApplied)
		rf.log = append(rf.log,
			LogEntry{
				Term:    term,
				Command: command,
			},
		)
		rf.applyMsgCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: index}

	}

	return index, term, isLeader
}

func (rf *Raft) waitForAppendEntries() {
	for rf.killed() == false {
		currentTerm, isLeader := rf.GetState()
		if isLeader {
			appendEntriesArgs := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[rf.me] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[rf.me]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			for i := range rf.peers {
				if i != rf.me {
					if len(rf.log)-1 >= rf.nextIndex[i] { // If last log index ≥ nextIndex for a follower
						go func(i int) {
							appendEntriesArgs.Entries = rf.log[rf.nextIndex[i]:]
							reply := AppendEntriesReply{}
							rf.sendAppendEntries(i,
								&appendEntriesArgs,
								&reply,
							)
							if reply.Success {
								rf.mu.Lock()
								defer rf.mu.Unlock()
								rf.nextIndex[i] += len(appendEntriesArgs.Entries)
								rf.matchIndex[i] = rf.nextIndex[i] - 1
							} else if reply.Term == currentTerm {
								rf.mu.Lock()
								defer rf.mu.Unlock()
								rf.nextIndex[i]--
							}
						}(i)
					}
				}
			}
			time.Sleep(50 * time.Millisecond)
			for {
				commitCount := 0
				for i := range rf.peers {
					if i != rf.me {
						if rf.matchIndex[i] > rf.commitIndex {
							commitCount++
						}
					}
				}
				Debugf(dLeader, "command %v committed for %d peers", rf.log[rf.commitIndex].Command, commitCount)
				if commitCount >= len(rf.peers)/2 {
					rf.commitIndex++
				} else {
					Debugf(dLeader, "agreement on command %v incomplete", rf.log[rf.commitIndex].Command)
					break
				}
				if rf.commitIndex == len(rf.log)-1 {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		currentTerm, isLeader := rf.GetState()

		if isLeader {
			// Check if the leader should send out heartbeats.
			if time.Since(rf.lastTimeHeard) > time.Duration(100*time.Millisecond) {
				Debugf(dLeader, "%v: I am the leader in term %v", rf.me, currentTerm)
				rf.mu.Lock()
				rf.lastTimeHeard = time.Now()
				rf.mu.Unlock()
				rf.sendHeartBeat()
			}

			// If the leader has not heard from the followers in a while,
			// start a new election.?
		} else if time.Since(rf.lastTimeHeard) > time.Duration(500*time.Millisecond) {
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.state = Candidates
			rf.mu.Unlock()

			Debugf(dLeader, "%v: started new election in term %v", rf.me, currentTerm+1)

			count := 0
			countMu := new(sync.Mutex)
			// var wg sync.WaitGroup
			for i := range rf.peers {
				if i != rf.me {
					// wg.Add(1)
					// Send a request to the next server.
					go func(i int) {
						// defer wg.Done()
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(i,
							&RequestVoteArgs{
								Term:         rf.currentTerm,
								CandidateId:  rf.me,
								LastLogIndex: len(rf.log) - 1,
								LastLogTerm:  rf.log[len(rf.log)-1].Term,
							},
							&reply,
						) // timeout would be 5s
						// Debugf(dVote, "%v: sending request vote to %v", rf.me, i)
						if ok && reply.VoteGranted {
							Debugf(dVote, "%v: received Vote from %v in term %v", rf.me, i, currentTerm+1)
							countMu.Lock()
							count++
							countMu.Unlock()
						}
						if !ok {
							Debugf(dVote, "%v: failed to connect to %v", rf.me, i)
						}
					}(i)
				}
			}
			// wg.Wait()
			time.Sleep(time.Duration(100 * time.Millisecond)) // wait 0.1s
			countMu.Lock()
			tmpCount := count
			countMu.Unlock()
			Debugf(dVote, "%v: Received %v votes in term %v\n", rf.me, tmpCount, currentTerm+1)
			if tmpCount >= len(rf.peers)/2 { // must wait for all goroutines to finish
				Debugf(dLeader, "%v: become leader in term %v\n", rf.me, currentTerm+1)
				rf.mu.Lock()
				rf.state = Leaders
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0 // initialize commit index
				}
				rf.mu.Unlock()
				rf.sendHeartBeat()
			} else {
				rf.mu.Lock()
				rf.state = Followers
				rf.mu.Unlock()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Term:    0,
		Command: nil,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Followers
	rf.lastTimeHeard = time.Now()
	rf.applyMsgCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.waitForAppendEntries()

	return rf
}

func (rf *Raft) sendHeartBeat() {
	// var wg sync.WaitGroup
	for i := range rf.peers {
		if i != rf.me {
			// Debugf(dLeader, "%v sending heartbeat to %v", rf.me, i)
			// wg.Add(1)
			go func(i int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i,
					&AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
						Entries:      []LogEntry{},
						LeaderCommit: rf.commitIndex,
					},
					&reply,
				)
				// wg.Done()
			}(i)
		}
	}
	// wg.Wait()
}
