package main

import (
"fmt"
"sync"
"io"
"math/rand"
"time"
)

// Constant represents the differenet state of the leader
const (
	follower = 1
	candidate = 2
	leader = 3
)

// Election time out variables
var (

MinimumElectionTimeout = 150
MaximumElectionTimeout = 2 * MinimumElectionTimeout

)

// ---------------------------------protected variables to maintain the consistency-------------------------------------

// Protected Int variable
type protectedInt struct {
	sync.RWMutex
	value int
}

func (i *protectedInt) setInt(value int) {
	i.Lock()
	defer i.Unlock()
	i.value = value;
}

func (i *protectedInt) getInt() int {
	i.Lock()
	defer i.Unlock()
	return i.value
}

type protectedBool struct {
	sync.RWMutex
	value bool
}

func (b *protectedBool) setBool(value bool) {
	b.Lock()
	defer b.Unlock()
	b.value = value;
}

func (b *protectedBool) getBool() bool {
	b.Lock()
	defer b.Unlock()
	return b.value
}

//------------------------------------------------------------------END-------------------------------------------------------------

//----------------------------------METHOD for timeout----------------------------------------

// electionTimeout returns a variable time.Duration, between the minimum and
// maximum election timeouts.
func electionTimeout() time.Duration {
	n := rand.Intn(int(MaximumElectionTimeout - MinimumElectionTimeout))
	d := int(MinimumElectionTimeout) + n
	return time.Duration(d) * time.Millisecond
}


// data structure for the indivisual server
type Server struct {

	serverId uint64 // unique server id of the server
	state *protectedInt // current state of the server
	runningStatus *protectedBool // whether server is running or not
	leader uint64 // keep the id of the leader
	term uint64 // current term number of the server
	vote uint64 // if applicable id of the server to whom server voted for the curreent term 
	log *serverLog // log of the server
	
	appendEntriesChannel chan appendEntriesReqRes // this require for sending the request for appendEntries or receiving its response
	voteChannel chan voteReqRes // require for sending vote request and receiving its response

	electionTimeOut chan time.Time // used when election are running and timeout happend
}




/* this method create a new server with the given id and readWriter. This method assumes that given 
 id is unique among the all created or running servers
 ServerId startswtih 1
 term starts with 0
 vote is 0(means vote is given to no one for the current term)
 */
func createNewServer(id uint64, rw io.ReadWriter) *Server {
	if id <=0 {
		panic("Server id is less than 0")
	}

	log := createNewLog(rw)
	latestTerm := log.lastTerm()

	s := &Server{
		serverId:	id,
		state:	&protectedInt{value: follower},
		runningStatus:	&protectedBool{value: false},
		leader:	0, // means no leader
		term:	latestTerm,
		vote:	0,
		log:	log,
		appendEntriesChannel:	make(chan appendEntriesReqRes),
		voteChannel:	make(chan voteReqRes),
	}

	return s
}

// This method run the server continusously until runningStatus is true
func (s *Server) run() {
	s.runningStatus.setBool(true)

	for s.runningStatus.getBool() {
		currentState := s.state.getInt()
		switch currentState {
		case 1:
			s.handleFollowerState()
		case 2:
			s.handleCandidateState()
		case 3:
			s.handleLeaderState()
		default:
			panic(fmt.Sprintf("Wrong Server state %d",currentState))
		}
	}
}

//------------------------------------------------------------------------------Methods to handle each state of the server-----------------------------------------

// This method handle the follower state
func (s *Server) handleFollowerState() {
	fmt.Printf("I am in follower state\n")
	for {
		select {

			case <-s.electionTick:
			// 5.2 Leader election: "A follower increments its current term and
			// transitions to candidate state."
			s.term++
			s.vote = noVote
			s.leader = unknownLeader
			s.state.Set(candidate)
			s.resetElectionTimeout()
			return

			case t := <-s.appendEntriesChan:
				if s.leader == unknownLeader {
					s.leader = t.Request.LeaderID
				}
				resp, stepDown := s.handleAppendEntries(t.Request)
				t.Response <- resp
				if stepDown {
					// stepDown as a Follower means just to reset the leader
					s.leader = t.Request.LeaderID
				}

			case t := <-s.requestVoteChan:
				resp, stepDown := s.handleRequestVote(t.Request)
				t.Response <- resp
				if stepDown {
					// stepDown as a Follower means just to reset the leader
					s.leader = unknownLeader
				}
		}
	}
}

// this method handle the leader state
func (s *Server) handleLeaderState() {
	fmt.Printf("I am in leader state\n")

	votes := map[uint64]bool{s.id: true}
	s.vote = s.id

	for {
		select {

			case t := <-s.appendEntriesChan:
			// "While waiting for votes, a candidate may receive an
			// appendEntries RPC from another server claiming to be leader.
			// If the leader's term (included in its RPC) is at least as
			// large as the candidate's current term, then the candidate
			// recognizes the leader as legitimate and steps down, meaning
			// that it returns to follower state."
			resp, stepDown := s.handleAppendEntries(t.Request)
			t.Response <- resp
			if stepDown {
				s.leader = t.Request.LeaderID
				s.state.Set(follower)
				return
			}

			
		}
	}

	s.runningStatus.setBool(false)
}

// this method handle the candidate state
func (s *Server) handleCandidateState() {
	fmt.Printf("I am in candidate state\n")
	s.state.setInt(leader)
}

//-------------------------------------------------------------------------------------END-----------------------------------------------------------------

// ---------------------------------------------------- Server start and stop methods------------------------------------

// this function start the server
func (s *Server) start() {
	s.run()
}

// this function stop the server
func (s *Server) stop() {
	s.runningStatus.setBool(false)
}

func main() {
	var rw io.ReadWriter
	s := createNewServer(1,rw)
	s.start()
}

//-------------------------------------------------END---------------------------------------------------------------------------------------

// handleAppendEntries will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=r.LeaderID, s.state.Set(Follower).
func (s *Server) handleAppendEntries(r appendEntries) (appendEntriesResponse, bool) {

	// If the request is from an old term, reject
	if r.term < s.term {
		return appendEntriesResponse{
			term:    s.term,
			success: false,
			reason:  fmt.Sprintf("Term %d < %d", r.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.term > s.term {
		s.term = r.Term
		s.vote = noVote
		stepDown = true
	}

	// Special case for candidates: "While waiting for votes, a candidate may
	// receive an appendEntries RPC from another server claiming to be leader.
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and steps down, meaning that it returns to follower state."
	if s.state.Get() == candidate && r.LeaderID != s.leader && r.Term >= s.term {
		s.term = r.Term
		s.vote = noVote
		stepDown = true
	}

	// In any case, reset our election timeout
	s.resetElectionTimeout()

	// Reject if log doesn't contain a matching previous entry
	if err := s.log.ensureLastIs(r.PrevLogIndex, r.PrevLogTerm); err != nil {
		return appendEntriesResponse{
			term:    s.term,
			success: false,
			reason: fmt.Sprintf(
				"while ensuring last log entry had index=%d term=%d: error: %s",
				r.PrevLogIndex,
				r.PrevLogTerm,
				err,
			),
		}, stepDown
	}

	// Process the entries
	for i, entry := range r.Entries {

		// Append entry to the log
		if err := s.log.appendlogEntry(entry); err != nil {
			return appendEntriesResponse{
				term:    s.term,
				success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(r.Entries),
					err,
				),
			}, stepDown
		}
	}

	// Commit up to the commit index.
	if r.commitIndex > 0 && r.commitIndex > s.log.getCommitIndex() {
		if err := s.log.commitTo(r.CommitIndex); err != nil {
			return appendEntriesResponse{
				Term:    s.term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
			}, stepDown
		}
	}

	// all good
	return appendEntriesResponse{
		Term:    s.term,
		Success: true,
	}, stepDown
}

// handleRequestVote will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=unknownLeader, s.state.Set(Follower).
func (s *Server) handleRequestVote(rv requestVote) (requestVoteResponse, bool) {
	// Spec is ambiguous here; basing this (loosely!) on benbjohnson's impl

	// If the request is from an old term, reject
	if rv.term < s.term {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", rv.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if rv.term > s.term {
		s.term = rv.term
		s.vote = noVote
		s.leader = unknownLeader
		stepDown = true
	}

	// Special case: if we're the leader, and we haven't been deposed by a more
	// recent term, then we should always deny the vote
	if s.state.Get() == leader && !stepDown {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      "already the leader",
		}, stepDown
	}

	// If we've already voted for someone else this term, reject
	if s.vote != 0 && s.vote != rv.CandidateID {
		if stepDown {
			panic("impossible state in handleRequestVote")
		}
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if s.log.lastIndex() > rv.prevLogIndex || s.log.lastTerm() > rv.prevLogTerm {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason: fmt.Sprintf(
				"our index/term %d/%d > %d/%d",
				s.log.lastIndex(),
				s.log.lastTerm(),
				rv.LastLogIndex,
				rv.LastLogTerm,
			),
		}, stepDown
	}

	// We passed all the tests: cast vote in favor
	s.vote = rv.CandidateID
	s.resetElectionTimeout()
	return requestVoteResponse{
		Term:        s.term,
		VoteGranted: true,
	}, stepDown
}


