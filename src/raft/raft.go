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
	"6.824/mylog"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	role           int                 //当前服务器的角色
	candidateTimer int
	pingMaxTime    int //单位毫秒
	maxReplyTime   int //单位毫秒

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//所有服务器都有的状态（persistent)
	currentTerm int        //服务器当前term
	votedFor    int        //当前term下，收到选票的候选者id
	log         []LogEntry //log数组，包括指令和收到指令时的term

	//所有服务器都有的状态（volatile)
	commitIndex int //已知的log数组中的最大下标
	lastApplied int //当前服务器的log数组下标

	//leaders的状态(volatile)
	nextIndex  []int //所有服务器的log数组中下一个下标
	matchIndex []int //所有服务器已知的log数组中的最大下标
}

type Role struct {
	role int
	mu   sync.Mutex
}

type LogEntry struct {
	commend string
	term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	role := rf.role

	return rf.currentTerm, role == LEADER
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId && rf.lastApplied <= args.LastLogIndex {
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term - 1

		mylog.Infof("me:%v term:%v votes for:%v \n", rf.me, rf.currentTerm, args.CandidateId)
		rf.role = FOLLOWER
		rf.candidateTimer = 0

		return
	}
}

type PingArgs struct {
	Term int //leader当前term
	Me   int //发起ping的id
}

type PingReply struct {
	Term int // follower或者candidate返回的term
}

func (rf *Raft) Ping(args *PingArgs, reply *PingReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.candidateTimer = 0
	if args.Term >= rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.Me
	}
	reply.Term = rf.currentTerm
	//rf.role = FOLLOWER
	//mylog.Infof("me:%v ping me:%v\n", args.Me, rf.me)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = FOLLOWER
	rf.votedFor = -1
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == CANDIDATE {
			requestVoteArgs := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastApplied,
				LastLogTerm:  0,
			}
			count := 0 //获得的选票数
			for i, peer := range rf.peers {
				requestVoteReply := &RequestVoteReply{Term: rf.currentTerm}
				if i == rf.me {
					continue
				}
				if !peer.Call("Raft.RequestVote", requestVoteArgs, requestVoteReply) {
					continue
				}
				if requestVoteReply.VoteGranted {
					count++
				} else if requestVoteReply.Term > rf.currentTerm {
					rf.currentTerm = requestVoteReply.Term
					//rf.role = FOLLOWER
					break
				}
			}
			mylog.Infof("me: %v 当前term:%v 被投票数: %v\n", rf.me, rf.currentTerm, count)
			if count+1 > len(rf.peers)/2 {
				mylog.Infof("me: %v len: %v term: %v", rf.me, len(rf.peers), rf.currentTerm)
				rf.role = LEADER
			} else {
				rf.votedFor = -1
			}
			//time.Sleep(time.Duration(rf.maxReplyTime) * time.Millisecond) //等待结果返回
		}
		if rf.role == LEADER {
			for i, peer := range rf.peers {
				if i != rf.me {
					args := &PingArgs{Term: rf.currentTerm,
						Me: rf.me}
					reply := &PingReply{}
					if !peer.Call("Raft.Ping", args, reply) {
						continue
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term - 1
						rf.role = FOLLOWER
						rf.votedFor = -1
						break
					}
				}
			}
		}
		rf.mu.Unlock()

		//time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
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
	rf.me = me
	rf.votedFor = -1
	rf.maxReplyTime = 200
	rf.role = FOLLOWER
	rand.Seed(time.Now().UnixNano())
	rf.pingMaxTime = rand.Int()%150 + 150 //ping的最长时间在150毫秒到300毫秒之间
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	mylog.SetLevel(mylog.InfoLevel)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.candidate_timer()
	return rf
}

// 超时后follower变成candidate
func (rf *Raft) candidate_timer() {
	for rf.killed() == false {
		//mylog.Infof("me %v 等待前timer: %v\n", rf.me, rf.candidateTimer)

		time.Sleep(time.Duration(rand.Int()%800+200) * time.Millisecond)
		//mylog.Infof("me %v 等待后timer: %v\n", rf.me, rf.candidateTimer)

		rf.mu.Lock()
		role := rf.role

		if role == LEADER {
			rf.mu.Unlock()
			continue
		}
		if rf.role == CANDIDATE { //重选举
			rf.role = FOLLOWER
			rf.currentTerm--
			rf.votedFor = -1
			rf.candidateTimer = 0
		}
		if rf.candidateTimer == 1 { //超时
			//角色变为candidate
			rf.currentTerm++
			rf.role = CANDIDATE
			rf.votedFor = rf.me
			mylog.Infof("%v 超时，当前term: %v\n", rf.me, rf.currentTerm)
		}
		rf.candidateTimer = 1
		rf.mu.Unlock()

	}
}
