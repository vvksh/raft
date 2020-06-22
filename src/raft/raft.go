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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	term     int         //current Term
	votedFor interface{} //identifier of whoever got vote last time , use clientEnd name which is a random string
	isLeader bool

	logs         []LogEntry // log entries this server contains
	lastLogIndex int        // index of last log in this server
	commitIndex  int        // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied  int        // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// state info for leaders, reinitialized after election
	nextIndices  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndices []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// election timer related info
	electionTimer        *time.Timer //election timer, when it goes off, a new election starts
	resetElectionTimerCh chan string
	stopElectionTimerCh  chan bool
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// RequestVoteArgs RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
//  RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler
// Decide whether or not to cast vote
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logf("request for vote from %d for term %d", args.CandidateId, args.Term)

	// only cast vote if
	// 1. candidate's term is at least up to date with this server's term
	// 2. this server didn't vote for anyone in this term or already voted for this candidate in this term
	// 3. the candidate's log is at least as long as the server's log

	if args.Term < rf.term {
		rf.logf("Didnt vote for %d; candidate term is lower, candidater's term: %d, my term : %d", args.CandidateId, args.Term, rf.term)
		reply.VoteGranted = false
		reply.Term = rf.term
		return
	}

	// if candidate is on next term, reset votedFor
	if args.Term > rf.term {
		rf.votedFor = nil
	}

	if rf.votedFor != nil && rf.votedFor != args.CandidateId {
		rf.logf("Didnt vote for %d; Already voted for %d", args.CandidateId, rf.votedFor)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	// else grant vote

	rf.term = args.Term
	rf.votedFor = args.CandidateId
	rf.logf("Granting vote to %d", args.CandidateId)
	reply.VoteGranted = true

	rf.resetElectionTimerCh <- "VOTED_FOR_CANDIDATE" //5.2, a server remains in follower state as long as it receives valid RPCs from a leader or candidate
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs struct is used by leader to send AppendEntriesRpc calls
type AppendEntriesArgs struct {
	Term     int // leader’s term
	LeaderId int //so follower can redirect clients
	// prevLogIndex int        //index of log entry immediately preceding new ones
	// prevLogTerm  int        // term of prevLogIndex entry
	Entries []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	// leaderCommit int        //leader commit index
}

type AppendEntriesReply struct {
	Term    int  // current term for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries receiver implementation
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
//    whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// 	  but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	candidateTerm := args.Term
	// rf.logf("heartbeat from leader; leader's term: %d", candidateTerm)
	currentTerm := rf.term
	// rf.logf("my term %d ", currentTerm)

	if candidateTerm < currentTerm {
		reply.Success = false
		return
	}

	// under partition if discovered a new leader, convert to a follower
	if candidateTerm > currentTerm {
		rf.mu.Lock()
		rf.isLeader = false
		rf.mu.Unlock()
	}

	// assume heartbeat for now and reset election timer

	rf.resetElectionTimerCh <- "RECEIVED_HEARTBEAT_FROM_LEADER"

	//todo 2., 3., 4. and 5

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderElectionThread() {
	rf.logf("start election thread")
	for {
		if rf.killed() {
			break
		}
		select {
		// case of reset timer
		// - followers reset timer when they get heartbeat
		// - candidates reset timer when they start election
		case resetReason := <-rf.resetElectionTimerCh:
			// stop timer if not already fired
			// if timer already fired but channel not drained drain it
			// See documentation for timer.Reset; documentation says to stop and drain before resetting; if channel already drained (drained in the case of election), it seems to get stuck there

			if !rf.electionTimer.Stop() { // stop returns false if timer was already stopped or expired; normal case when a candidate starts election
				rf.logf("case: reseting; Seems like election timer expired or stopped already, draining channel just in case")
				// <-rf.electionTimer.C  // seems like its stuck when candidate resets because this channel is already drained //TODO check and remove
			}
			newDuration := getNewElectionTimerDuration()
			rf.electionTimer.Reset(newDuration)
			rf.logf("reset election timer due to %s; new timer should expire at %s", resetReason, time.Now().Add(newDuration).Format("2006-01-02 15:04:05.000"))

		// case of when election timer fires
		// - followers didn't get heartbeat from leader
		// - candidate didn't get elected
		case <-rf.electionTimer.C:
			rf.logf("Election timer went off")
			go rf.startLeaderElection()

		// case: stop election timer when a candidate is elected leader; note the candidate resets the timer
		case <-rf.stopElectionTimerCh:
			rf.logf("stopping election timer")
			// stop and drain election timer channel if necessary
			stopped := rf.electionTimer.Stop()
			if stopped {
				rf.logf("case: stopped; stopped an active timer")
			} else {
				rf.logf("case: stopped; seems like timer was already stopped")
			}
		}
	}

}

func (rf *Raft) sendHeartBeats() {
	for {
		rf.mu.Lock()
		isLeader := rf.isLeader
		isKilled := rf.killed()
		rf.mu.Unlock()

		//stop sending heartbeats if not leader
		if !isLeader || isKilled {
			rf.logf("Stop sending heartbeats, status - isLeader %v, isKilled %v", isLeader, isKilled)
			break
		}
		for serverIndex, _ := range rf.peers {

			if serverIndex == rf.me {
				// skip self, already voted for self
				continue
			}
			// rf.logf("sending heartbeat to %d", serverIndex)
			heartbeatArgs := AppendEntriesArgs{}
			heartbeatArgs.LeaderId = rf.me
			heartbeatArgs.Term = rf.term
			rf.logf("Sending heartbeat to Node: %d; args: %#v", serverIndex, heartbeatArgs)
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(serverIndex, &heartbeatArgs, &reply)
		}

		time.Sleep(HEART_BEAT_INTERVAL_MILLIS * time.Millisecond)
	}
}

// election timeout passed without any heartbeat; so change to a candidate
// - On conversion to candidate, start election:
// 		- Increment currentTerm
// 		- Vote for self
// 		- Reset election timer
// 		- Send RequestVote RPCs to all other servers
// - If votes received from majority of servers: become leader
// - If AppendEntries RPC received from new leader: convert to
// 	 follower
// - If election timeout elapses: start new election
func (rf *Raft) startLeaderElection() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.logf("started new election. number of peers %d", len(rf.peers))
	// numVotesMu := &sync.Mutex{} // mutex for setting number of votes
	rf.term++
	go func() { rf.resetElectionTimerCh <- "START_LEADER_ELECTION" }()
	numvotes := 1
	rf.votedFor = rf.me

	// rf.resetElectionTimerCh <- true
	for serverIndex, _ := range rf.peers {

		rf.logf("Requesting vote loop, current server %d", serverIndex)
		if serverIndex == rf.me {
			// skip self, already voted for self
			continue
		}
		// go func() {
		args := RequestVoteArgs{}
		args.CandidateId = rf.me
		args.Term = rf.term
		args.LastLogTerm = rf.getLastLogterm()
		args.LastLogIndex = rf.lastLogIndex

		reply := RequestVoteReply{}
		rf.logf("Sending rfc for sendRequestVote to %d", serverIndex)

		if rf.sendRequestVote(serverIndex, &args, &reply) {

			if !reply.VoteGranted {
				if reply.Term > rf.term {
					rf.mu.Lock()
					rf.term = reply.Term
					rf.mu.Unlock()
				}
			} else {
				// numVotesMu.Lock()
				numvotes++
				// numVotesMu.Unlock()
			}
		} else {
			rf.logf("unsuccessful rfc for sendRequestVote to %d", serverIndex)
		}
		// }()
	}

	if elected(numvotes, len(rf.peers)) {
		rf.logf("Elected leader")
		rf.isLeader = true
		go func() { rf.stopElectionTimerCh <- true }()
		go rf.sendHeartBeats()
	}

}

func elected(numVotes int, total int) bool {
	if numVotes > total/2 {
		return true
	}
	return false
}

func (rf *Raft) getLastLogterm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	return lastLogTerm
}

func (rf *Raft) logf(format string, v ...interface{}) {
	message := fmt.Sprintf("time: %s: Node#%d ; %s \n", getCurrentTimeString(), rf.me, format)
	fmt.Printf(message, v...)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.term = 0
	rf.me = me
	electionTimerDuration := getNewElectionTimerDuration()
	rf.electionTimer = time.NewTimer(electionTimerDuration) //this starts an election timer
	rf.resetElectionTimerCh = make(chan string)
	rf.stopElectionTimerCh = make(chan bool)

	rf.logf("Initialized new raft node with election_timer: %v ", electionTimerDuration.String())
	// Your initialization code here (2A, 2B, 2C).
	go rf.leaderElectionThread()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
