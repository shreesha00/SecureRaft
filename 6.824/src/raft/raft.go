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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	crand "crypto/rand"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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

type LogEntry struct {
	Term     int
	Index    int
	Command  interface{}
	KeyEntry KeyBlock
}

const ETC int = 10          // Election Timeout Checker period
const AT int = 10           // Applier thread timeout value in milliseconds
const HTC int = 100         // Heartbeat Timer period in milliseconds
const TIMEOUT_MIN int = 300 // lower bound of election timeout range
const TIMEOUT_MAX int = 500 // upper bound of election timeout range
const KEYCHANGE int = 50    // change keys every 50 committed log entries

//
// State typedef for readability
//
type State int

const follower, candidate, leader State = 0, 1, 2

// key block
// AES-256 GCM AEAD is used
type KeyBlock struct {
	ChangeIndex int    // index at which this key was changed
	Key         []byte // secret symmetric key to be used for authenticated encryption
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm      int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor         int        // candidateId that received vote in current term (or null if none)
	log              []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	electionKeyBlock KeyBlock   // key block used to encrypt and authenticate election messages, append entries header

	lastHeardTime   time.Time // time when current peer last heard from a leader
	electionTimeout int64     // election timeout period in milliseconds
	state           State     // state of the current peer

	// volatile state
	commitIndex int // index of the highest log entry known to be commited (initialized to 0, increases monotonically)
	lastApplied int // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state specific to leaders. Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

// returns a random timeout value in milliseconds
func getRandomTimeout() int64 {
	timeout := int64(rand.Float64()*(float64(TIMEOUT_MAX-TIMEOUT_MIN)) + float64(TIMEOUT_MIN))
	return timeout
}

// get latest key block. Assumes lock is held. Does not release lock
func (rf *Raft) getLatestKeyBlock(index int) KeyBlock {
	return rf.log[int(index/KEYCHANGE)*KEYCHANGE].KeyEntry
}

// attempt a new Leader Election
func (rf *Raft) attemptElection() {
	rf.mu.Lock()
	rf.state = candidate                      // state changes to candidate
	rf.currentTerm++                          // increment current term
	rf.votedFor = rf.me                       // vote for itself
	rf.lastHeardTime = time.Now()             // reset election timer
	rf.electionTimeout = getRandomTimeout()   // reset election timer
	term := rf.currentTerm                    // save current term for RPC call
	lastLogIndex := len(rf.log) - 1           // last log index
	lastLogTerm := rf.log[len(rf.log)-1].Term // last log term
	rf.persist()                              // persist state to persister
	rf.mu.Unlock()

	DPrintf("%v started an election at term %v", rf.me, term)
	// counter setup for counting votes
	votes := 1
	total := 1
	var countVoteLock sync.Mutex                 // mutex to lock vote count variables
	condVoteLock := sync.NewCond(&countVoteLock) // condition variable to signal changes

	// send request vote RPCs to peers in seperate threads
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me /* safe to refer to variable protected by lock as this variable is only written once at the beginning and never touched again */ {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			go func(server int) {
				vote := rf.sendRequestVote(server, &args, &reply)
				countVoteLock.Lock()
				defer countVoteLock.Unlock()
				if vote {
					votes++
				}
				total++
				condVoteLock.Broadcast()
			}(i)
		}
	}

	countVoteLock.Lock()
	defer countVoteLock.Unlock()
	for votes <= len(rf.peers)/2 && total != len(rf.peers) {
		condVoteLock.Wait()
	}
	rf.mu.Lock()
	if votes > len(rf.peers)/2 && rf.currentTerm == term {
		rf.state = leader // leader elected

		DPrintf("%v becomes leader at term %v", rf.me, rf.currentTerm)
		// initialize matchIndex and nextIndex
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}

		// send empty Append Entry RPCs
		rf.doAppendEntries()
	} else {
		rf.mu.Unlock()
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// function to add crypto wrapper to RequestVoteArgs structure.
func (rf *Raft) addWrapperRequestVoteArgs(args *RequestVoteArgs, cryptoArgs *RequestVoteArgsCrypto) {
	key := rf.electionKeyBlock.Key

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(args) != nil {
		panic("could not encode RequestVoteArgs structure")
	}
	plaintext := w.Bytes()

	nonce := make([]byte, 12)
	if _, err := io.ReadFull(crand.Reader, nonce); err != nil {
		panic(err.Error())
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("addWrapperRequestVoteArgs %d", len(nonce))
	ciphertext := aesgcm.Seal(nil, nonce, plaintext, nil)

	cryptoArgs.Ciphertext = ciphertext
	cryptoArgs.Nonce = nonce

}

// function to remove crypto wrapper from RequestVoteArgsCrypto structure.
func (rf *Raft) removeWrapperRequestVoteArgs(cryptoArgs *RequestVoteArgsCrypto, args *RequestVoteArgs) {
	key := rf.electionKeyBlock.Key

	ciphertext := cryptoArgs.Ciphertext
	nonce := cryptoArgs.Nonce

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("removeWrapperRequestVoteArgs %d", len(nonce))
	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)

	if err != nil {
		panic(err.Error())
	}

	r := bytes.NewBuffer(plaintext)
	d := labgob.NewDecoder(r)
	if d.Decode(args) != nil {
		panic("could not decode received plaintext into RequestVoteArgs structure")
	}

}

// function to add crypto wrapper to RequestVoteReply structure.
func (rf *Raft) addWrapperRequestVoteReply(args *RequestVoteReply, cryptoArgs *RequestVoteReplyCrypto) {
	key := rf.electionKeyBlock.Key

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(args) != nil {
		panic("could not encode RequestVoteReply structure")
	}
	plaintext := w.Bytes()

	nonce := make([]byte, 12)
	if _, err := io.ReadFull(crand.Reader, nonce); err != nil {
		panic(err.Error())
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("addWrapperRequestVoteReply %d", len(nonce))
	ciphertext := aesgcm.Seal(nil, nonce, plaintext, nil)

	cryptoArgs.Ciphertext = ciphertext
	cryptoArgs.Nonce = nonce

}

// function to remove crypto wrapper from RequestVoteReply structure.
func (rf *Raft) removeWrapperRequestVoteReply(cryptoArgs *RequestVoteReplyCrypto, args *RequestVoteReply) {
	key := rf.electionKeyBlock.Key

	ciphertext := cryptoArgs.Ciphertext
	nonce := cryptoArgs.Nonce

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("removeWrapperRequestVoteReply %d", len(nonce))
	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)

	if err != nil {
		panic(err.Error())
	}

	r := bytes.NewBuffer(plaintext)
	d := labgob.NewDecoder(r)
	if d.Decode(args) != nil {
		panic("could not decode received plaintext into RequestVoteReply structure")
	}

}

// function to add crypto wrapper to AppendEntriesArgs structure. Assumes lock is held, does not release it
func (rf *Raft) addWrapperAppendEntriesArgs(args *AppendEntriesArgs, cryptoArgs *AppendEntriesArgsCrypto) {
	key_header := rf.electionKeyBlock.Key

	key_entries := rf.getLatestKeyBlock(args.PrevLogIndex).Key

	argsWOEntries := AppendEntriesArgsWOLogEntries{
		Term:         args.Term,
		LeaderId:     args.LeaderId,
		LeaderCommit: args.LeaderCommit,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
	}

	w_header := new(bytes.Buffer)
	e_header := labgob.NewEncoder(w_header)
	if e_header.Encode(argsWOEntries) != nil {
		panic("could not encode AppendEntriesArgs without entries structure")
	}

	plaintext_header := w_header.Bytes()

	nonce_header := make([]byte, 12)
	if _, err := io.ReadFull(crand.Reader, nonce_header); err != nil {
		panic(err.Error())
	}

	block_header, err := aes.NewCipher(key_header)
	if err != nil {
		panic(err.Error())
	}

	aesgcm_header, err := cipher.NewGCM(block_header)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("addWrapperAppendEntriesArgs 1 %d", len(nonce_header))
	ciphertext_header := aesgcm_header.Seal(nil, nonce_header, plaintext_header, nil)

	w_entries := new(bytes.Buffer)
	e_entries := labgob.NewEncoder(w_entries)
	if e_entries.Encode(args.Entries) != nil {
		panic("could not encode AppendEntriesArgs log entries")
	}

	plaintext_entries := w_entries.Bytes()

	nonce_entries := make([]byte, 12)
	if _, err := io.ReadFull(crand.Reader, nonce_entries); err != nil {
		panic(err.Error())
	}

	block_entries, err := aes.NewCipher(key_entries)
	if err != nil {
		panic(err.Error())
	}

	aesgcm_entries, err := cipher.NewGCM(block_entries)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("addWrapperAppendEntriesArgs 2 %d", len(nonce_entries))
	ciphertext_entries := aesgcm_entries.Seal(nil, nonce_entries, plaintext_entries, nil)

	cryptoArgs.CiphertextHeader = ciphertext_header
	cryptoArgs.NonceHeader = nonce_header
	cryptoArgs.CipherTextLogEntries = ciphertext_entries
	cryptoArgs.NonceLogEntries = nonce_entries

}

// function to add crypto wrapper to AppendEntriesReply structure.
func (rf *Raft) addWrapperAppendEntriesReply(args *AppendEntriesReply, cryptoArgs *AppendEntriesReplyCrypto) {
	key := rf.electionKeyBlock.Key

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(args) != nil {
		panic("could not encode AppendEntriesReply structure")
	}
	plaintext := w.Bytes()

	nonce := make([]byte, 12)
	if _, err := io.ReadFull(crand.Reader, nonce); err != nil {
		panic(err.Error())
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("addWrapperAppendEntriesReply %d", len(nonce))
	ciphertext := aesgcm.Seal(nil, nonce, plaintext, nil)

	cryptoArgs.Ciphertext = ciphertext
	cryptoArgs.Nonce = nonce

}

// function to remove crypto wrapper from AppendEntriesReply structure.
func (rf *Raft) removeWrapperAppendEntriesReply(cryptoArgs *AppendEntriesReplyCrypto, args *AppendEntriesReply) {
	key := rf.electionKeyBlock.Key

	ciphertext := cryptoArgs.Ciphertext
	nonce := cryptoArgs.Nonce

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("removeWrapperAppendEntriesReply %d", len(nonce))
	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)

	if err != nil {
		panic(err.Error())
	}

	r := bytes.NewBuffer(plaintext)
	d := labgob.NewDecoder(r)
	if d.Decode(args) != nil {
		panic("could not decode received plaintext into AppendEntriesReply structure")
	}

}

//
// periodically checks if current peer has timed out and if so starts a leader election
//
func (rf *Raft) periodicLeaderElection() {
	for !rf.killed() {
		time.Sleep(time.Duration(ETC) * time.Millisecond)
		rf.mu.Lock()
		diff := int64(time.Since(rf.lastHeardTime) / time.Millisecond) // time difference between current time and time at which the current peer heard from any other peer
		if (rf.state == follower || rf.state == candidate) && diff >= rf.electionTimeout {
			rf.mu.Unlock()
			go rf.attemptElection() // launch an election in a separate thread
			continue
		}
		rf.mu.Unlock()
	}
}

//
// periodically applies commited log entries
//
func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(time.Duration(AT) * time.Millisecond)
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			newApplyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			applyCh <- newApplyMsg
		}
		rf.mu.Unlock()
	}
}

//
// send AppendEntries RPC to all peers. Lock must be held before calling. Lock is guarenteed to be released before exit
//
func (rf *Raft) doAppendEntries() {
	// prepare arguments for append entry
	arguments := []AppendEntriesArgs{}
	for i := 0; i < len(rf.peers); i++ {
		entries := rf.log[rf.nextIndex[i]:]
		newArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			LeaderCommit: rf.commitIndex,
		}
		newArgs.Entries = make([]LogEntry, len(entries))
		// make a deep copy of the entries to send
		copy(newArgs.Entries, entries)
		arguments = append(arguments, newArgs)
	}
	rf.mu.Unlock()

	// send AppendEntries RPCs to peers in seperate threads
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				args := arguments[server]
				var reply AppendEntriesReply
				rf.sendAppendEntries(server, &args, &reply)
			}(i)
		}
	}
}

//
// periodically send out AppendEntries RPCs if current peer is leader
//
func (rf *Raft) periodicAppendEntries() {
	for !rf.killed() {
		time.Sleep(time.Duration(HTC) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == leader {
			rf.doAppendEntries() // releases lock
			continue
		}
		rf.mu.Unlock()
	}
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		panic("could not save persistent state")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("could not restore persisted state")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote (index in peers)
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int  // leader's term
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// additional data for accelerated log backtracking optimization
	ConflictIndex int
	ConflictTerm  int
}

//
// RequestVote RPC arguments structure with Crypto wrapper
//
type RequestVoteArgsCrypto struct {
	Ciphertext []byte
	Nonce      []byte
}

//
// RequestVote RPC reply structure with Crypto wrapper
//
type AppendEntriesArgsCrypto struct {
	CiphertextHeader     []byte
	NonceHeader          []byte
	CipherTextLogEntries []byte
	NonceLogEntries      []byte
}

//
// AppendEntries RPC arguments structure with Crypto wrapper
//
type AppendEntriesReplyCrypto struct {
	Ciphertext []byte
	Nonce      []byte
}

//
// AppendEntries RPC reply structure with Crypto wrapper
//
type RequestVoteReplyCrypto struct {
	Ciphertext []byte
	Nonce      []byte
}

//
// AppendEntriesArgsWOLogEntries RPC arguments structure
//
type AppendEntriesArgsWOLogEntries struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of PrevLogIndex entry
	LeaderCommit int // leader’s commitIndex
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(cryptoArgs *RequestVoteArgsCrypto, cryptoReply *RequestVoteReplyCrypto) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	args := RequestVoteArgs{}
	rf.removeWrapperRequestVoteArgs(cryptoArgs, &args)
	// if my term is out-dated step down to follower state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
	}

	reply := RequestVoteReply{}
	reply.Term = rf.currentTerm

	// if term of sender is out-dated, do not grant vote
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.addWrapperRequestVoteReply(&reply, cryptoReply)
		return
	}

	//
	// grant vote only if already voted for no one or the same candidate and candidate log is at-least as up-to-date as my own
	// refer to paper for specfic definition of "at-least as up-to-date"
	//
	if ((rf.votedFor == -1) || (rf.votedFor == args.CandidateId)) && ((args.LastLogTerm > rf.log[len(rf.log)-1].Term) || ((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (args.LastLogIndex+1 >= len(rf.log)))) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeardTime = time.Now()
	} else {
		reply.VoteGranted = false
	}

	rf.addWrapperRequestVoteReply(&reply, cryptoReply)
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(cryptoArgs *AppendEntriesArgsCrypto, cryptoReply *AppendEntriesReplyCrypto) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	key_header := rf.electionKeyBlock.Key

	ciphertext_header := cryptoArgs.CiphertextHeader
	nonce_header := cryptoArgs.NonceHeader

	block_header, err := aes.NewCipher(key_header)
	if err != nil {
		panic(err.Error())
	}

	aesgcm_header, err := cipher.NewGCM(block_header)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("AppendEntries 1 %d", len(nonce_header))
	plaintext_header, err := aesgcm_header.Open(nil, nonce_header, ciphertext_header, nil)

	if err != nil {
		panic(err.Error())
	}

	headerArgs := AppendEntriesArgsWOLogEntries{}
	r := bytes.NewBuffer(plaintext_header)
	d := labgob.NewDecoder(r)
	if d.Decode(&headerArgs) != nil {
		panic("could not decode received plaintext into AppendEntriesArgs structure without log entries")
	}

	// if my term is out-dated step down to follower state
	if headerArgs.Term > rf.currentTerm {
		rf.currentTerm = headerArgs.Term
		rf.state = follower
		rf.votedFor = -1
	}

	reply := AppendEntriesReply{}
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// if term of sender is out-dated, consider it to be un-successful Append Entry
	if headerArgs.Term < rf.currentTerm {
		reply.Success = false
		rf.addWrapperAppendEntriesReply(&reply, cryptoReply)
		return
	} else if rf.state == candidate {
		rf.state = follower
		rf.votedFor = -1
	}

	// reset election timer as we have heard from current leader
	rf.lastHeardTime = time.Now()

	if headerArgs.PrevLogIndex >= len(rf.log) { // if PrevLogIndex is beyond my log, ConflictIndex is set to len(log). This ensures that the next AppendEntries will have a valid PrevLogIndex
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		rf.addWrapperAppendEntriesReply(&reply, cryptoReply)
		return
	}

	if rf.log[headerArgs.PrevLogIndex].Term != headerArgs.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[headerArgs.PrevLogIndex].Term

		// find index of first entry whose term is ConflictTerm
		ind := headerArgs.PrevLogIndex
		for ; ind >= 0; ind-- {
			if rf.log[ind].Term != reply.ConflictTerm {
				break
			}
		}
		// set ConflictIndex to index of first entry whose term is ConflictTerm. This ensures that the next AppendEntries will either conflict with a different term or succeed thereby essentially skipping over one entire term when back-tracking
		reply.ConflictIndex = ind + 1
		rf.addWrapperAppendEntriesReply(&reply, cryptoReply)
		return
	}

	key_entries := rf.getLatestKeyBlock(headerArgs.PrevLogIndex).Key

	ciphertext_entries := cryptoArgs.CipherTextLogEntries
	nonce_entries := cryptoArgs.NonceLogEntries

	block_entries, err := aes.NewCipher(key_entries)
	if err != nil {
		panic(err.Error())
	}

	aesgcm_entries, err := cipher.NewGCM(block_entries)
	if err != nil {
		panic(err.Error())
	}

	DPrintf("Append Entries 2 %d", len(nonce_entries))
	plaintext_entries, err := aesgcm_entries.Open(nil, nonce_entries, ciphertext_entries, nil)

	if err != nil {
		panic(err.Error())
	}

	var logEntries []LogEntry
	r = bytes.NewBuffer(plaintext_entries)
	d = labgob.NewDecoder(r)
	if d.Decode(&logEntries) != nil {
		panic("could not decode received plaintext into Log Entries for AppendEntriesArgs")
	}

	// loops from PrevLogIndex + 1 till end of log until either a term mis-match occurs with new Entries or we reach the end of the log
	var i, j int
	for i, j = headerArgs.PrevLogIndex+1, 0; i < len(rf.log) && j < len(logEntries); i, j = i+1, j+1 {
		if rf.log[i].Term != logEntries[j].Term {
			break
		}
	}

	if j < len(logEntries) { // term mis-match occurs

		// truncate log at the point of term mis-match and append the rest of the entries
		rf.log = rf.log[:i]                        // truncate log
		rf.log = append(rf.log, logEntries[j:]...) // append the rest of the entries
	}

	// index of last new entry added
	lastNewEntryIndex := headerArgs.PrevLogIndex + len(logEntries)

	// update commitIndex if Leaders log is ahead
	if headerArgs.LeaderCommit > rf.commitIndex {
		if headerArgs.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = headerArgs.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
	}

	// reset election timer
	reply.Success = true
	rf.addWrapperAppendEntriesReply(&reply, cryptoReply)

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
// look at the comments in ../labrpc/labrpc.go for more details.getbacklok
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%v sent request vote to %v. Arguments are Term: %v, LastLogIndex: %v, LastLogTerm: %v", rf.me, server, args.Term, args.LastLogIndex, args.LastLogTerm)

	cryptoArgs := RequestVoteArgsCrypto{}
	rf.addWrapperRequestVoteArgs(args, &cryptoArgs)
	cryptoReply := RequestVoteReplyCrypto{}
	ok := rf.peers[server].Call("Raft.RequestVote", &cryptoArgs, &cryptoReply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.removeWrapperRequestVoteReply(&cryptoReply, reply)
	DPrintf("%v received request vote reply from %v at term %v. Arguments are Term: %v, LastLogIndex: %v, LastLogTerm: %v. Reply contains Term: %v, VoteGranted: %v", rf.me, server, rf.currentTerm, args.Term, args.LastLogIndex, args.LastLogTerm, reply.Term, reply.VoteGranted)
	if rf.state != candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
	}

	return reply.VoteGranted
}

//
// send a AppendEntries RPC and handle the reply from the peer
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v sent append entries to %v. Arguments are Term: %v, LeaderCommit: %v, PrevLogIndex: %v, PrevLogTerm: %v", rf.me, server, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm)

	cryptoArgs := AppendEntriesArgsCrypto{}
	rf.mu.Lock()
	rf.addWrapperAppendEntriesArgs(args, &cryptoArgs)
	rf.mu.Unlock()
	cryptoReply := AppendEntriesReplyCrypto{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &cryptoArgs, &cryptoReply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.removeWrapperAppendEntriesReply(&cryptoReply, reply)
	DPrintf("%v recieved append entries reply from %v. Arguments are Term: %v, LeaderCommit: %v, PrevLogIndex: %v, PrevLogTerm: %v. Reply contains Term: %v, ConflictIndex: %v, ConflictTerm: %v, Success: %v", rf.me, server, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, reply.Term, reply.ConflictIndex, reply.ConflictTerm, reply.Success)
	if rf.state != leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm { // takes care of the case where the AppendEntries RPC was rejected due to an out-of-date term
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = -1
		return
	}

	// failure implies log inconsistencies
	if reply.Success == true {
		// update new index till which the log matches
		newMatchIndex := args.PrevLogIndex + len(args.Entries)

		// update matchIndex only if the new index is greater than the previous. Takes care of old messages
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}

		// set nextIndex one beyond the matchIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		// if conflictTerm is -1, set nextIndex to conflictIndex. For fast back-tracking
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// find first index beyond the last index with conflictTerm
			ind := len(rf.log) - 1
			for ind >= 0 && rf.log[ind].Term != reply.ConflictTerm {
				ind--
			}
			if ind >= 0 { // set nextIndex to first index beyond the last index with conflictTerm
				rf.nextIndex[server] = ind + 1
			} else { // no entry in current log with conflictTerm
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	}

	// update commitIndex
	for N := len(rf.log) - 1; N >= rf.commitIndex; N-- {
		count := 1
		if rf.log[N].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			break
		}
	}
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
	isLeader := false

	// Your code here (2B).
	if !rf.killed() {
		rf.mu.Lock()
		isLeader = rf.state == leader
		if isLeader {
			if len(rf.log)%KEYCHANGE == 0 {
				new_key := make([]byte, 32)
				if _, err := io.ReadFull(crand.Reader, new_key); err != nil {
					panic(err.Error())
				}

				newKeyBlock := KeyBlock{
					ChangeIndex: len(rf.log),
					Key:         new_key,
				}

				newKeyEntry := LogEntry{
					Index:    len(rf.log),
					Term:     rf.currentTerm,
					KeyEntry: newKeyBlock,
					Command:  command,
				}
				rf.log = append(rf.log, newKeyEntry)
				term = newKeyEntry.Term
				index = newKeyEntry.Index
				DPrintf("New key entry added at leader %v at term %v at index %v", rf.me, term, index)
				rf.persist()
			} else {
				newLogEntry := LogEntry{
					Index:   len(rf.log),
					Term:    rf.currentTerm,
					Command: command,
				}
				rf.log = append(rf.log, newLogEntry)
				term = newLogEntry.Term
				index = newLogEntry.Index
				DPrintf("New command added at leader %v at term %v at index %v", rf.me, term, index)
				rf.persist()
			}
		}
		rf.mu.Unlock()
	}

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
	DPrintf("%v killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).

	// seed the random generator with the server ID so that different servers get different sequence of timeout values
	rand.Seed(int64(me))

	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.electionKeyBlock = KeyBlock{
		ChangeIndex: 0,
		Key:         []byte("2s5u8x/A?D(G+KbPeShVmYq3t6w9z$B&"),
	}

	// add a dummy entry in the log with index 0 so that the first valid index is 1

	keyBlock := KeyBlock{
		ChangeIndex: 0,
		Key:         []byte("KbPeShVmYq3t6w9z$B&E)H@McQfTjWnZ"),
	}

	rf.log = []LogEntry{}
	dummyEntry := LogEntry{
		Term:     0,
		Index:    0,
		KeyEntry: keyBlock,
	}
	rf.log = append(rf.log, dummyEntry)

	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastHeardTime = time.Now()           // setup timer
	rf.electionTimeout = getRandomTimeout() // setup random timeout valie
	rf.state = follower                     // all peers start in follower state

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// launch threads for periodic Leader Election and AppendEntries (if current peer is a leader)
	go rf.periodicLeaderElection()
	go rf.periodicAppendEntries()

	// launch thread for the periodic application of commit log entries
	go rf.applier(applyCh)

	return rf
}
