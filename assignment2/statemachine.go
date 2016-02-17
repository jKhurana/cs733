package main
import "fmt"

// Constant represents the differenet state of the leader
const (
	follower = 1
	candidate = 2
	leader = 3
)

const (
noVote = 0
unknownLeader = 0
)

type VoteReqEv struct {
	term uint64
	candidateId uint64
	prevLogTerm uint64
	prevLogIndex uint64
}

type Timeout struct {
}

type AppendEntriesReqEv struct {
	term uint64 
	prevLogTerm uint64
	prevLogIndex uint64
	leaderId uint64
	logRecord []logEntry
	commitIndex uint64
}

type AppendEntriesResEv struct {
	term uint64
	success bool
	reason string
}

type VoteResEv struct {
	term uint64
	voteGranted bool
	reason string
}


type StateMachine struct {
	serverId uint64 // unique server id of the server
	peer []int // peers id
	state uint // current state of the server
	leader uint64 // keep the id of the leader
	term uint64 // current term number of the server
	voteFor uint64 // if applicable id of the server to whom server voted for the curreent term 
	log *serverLog // log of the server
}

func main() {
	// testing testing
	var sm StateMachine
	sm.ProcessEvent(AppendEntriesReqEv{term : 10, prevLogIndex: 100, prevLogTerm: 3})
}

func (sm *StateMachine) ProcessEvent (ev interface{}) {
	switch ev.(type) {
	
	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)
		// "While waiting for votes, a candidate may receive an
		// appendEntries RPC from another server claiming to be leader.
		// If the leader's term (included in its RPC) is at least as
		// large as the candidate's current term, then the candidate
		// recognizes the leader as legitimate and steps down, meaning
		// that it returns to follower state."
		resp, stepDown := s.handleAppendEntries(AppendEntriesReqEv)
		t.Response <- resp
		if stepDown {
			s.leader = t.Request.LeaderID
			s.state = follower
			return
		}	
		fmt.Printf("%v\n", cmd)
	case VoteReqEv:
		cmd := ev.(VoteReqEv)
		// do stuff with req
		fmt.Printf("%v\n", cmd)

	// other cases
	default: println ("Unrecognized")
	}
}

// handleAppendEntries will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=r.LeaderID, s.state.Set(Follower).
func (sm *StateMachine) handleAppendEntries(r AppendEntriesReqEv) (AppendEntriesResEv, bool) {

	// If the request is from an old term, reject
	if r.term < sm.term {
		return AppendEntriesResEv{
			term:    sm.term,
			success: false,
			reason:  fmt.Sprintf("Term %d < %d", r.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.term > sm.term {
		sm.term = r.term
		sm.vote = noVote
		stepDown = true
	}

	// Special case for candidates: "While waiting for votes, a candidate may
	// receive an appendEntries RPC from another server claiming to be leader.
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and steps down, meaning that it returns to follower state."
	if sm.state == candidate && r.leaderId != s.leader && r.term >= sm.term {
		s.term = r.term
		s.vote = noVote
		stepDown = true
	}

	// In any case, reset our election timeout
	//s.resetElectionTimeout()

	// Reject if log doesn't contain a matching previous entry
	if err := sm.log.ensureLastIndexandTerm(r.prevLogIndex, r.prevLogTerm); err != nil {
		return AppendEntriesResEv{
			term:    sm.term,
			success: false,
			reason: fmt.Sprintf(
				"while ensuring last log entry had index=%d term=%d: error: %s",
				r.prevLogIndex,
				r.prevLogTerm,
				err,
			),
		}, stepDown
	}

	// Process the entries
	for i, entry := range r.logRecord {

		// Append entry to the log
		if err := s.log.appendlogEntry(entry); err != nil {
			return AppendEntriesResEv{
				term:    sm.term,
				success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(r.logRecord),
					err,
				),
			}, stepDown
		}
	}

	// Commit up to the commit index.
	if r.commitIndex > 0 && r.commitIndex > sm.log.commitIndex() {
		if err := s.log.commitTo(r.CommitIndex); err != nil {
			return AppendEntriesResEv{
				Term:    s.term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
			}, stepDown
		}
	}

	// all good
	return AppendEntriesResEv{
		Term:    sm.term,
		Success: true,
	}, stepDown
}


// handleRequestVote will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=unknownLeader, s.state.Set(Follower).
func (sm *StateMachine) handleRequestVote(rv VoteReqEv) (VoteResEv, bool) {

	// If the request is from an old term, reject
	if rv.term < sm.term {
		return VoteResEv{
			term:        s.term,
			voteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", rv.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if rv.term > sm.term {
		sm.term = rv.term
		sm.vote = noVote
		sm.leader = unknownLeader
		stepDown = true
	}

	// if we're the leader, and we haven't been deposed by a more
	// recent term, then we should always deny the vote
	if sm.state == leader && !stepDown {
		return VoteResEv{
			term:        sm.term,
			voteGranted: false,
			reason:      "already the leader",
		}, stepDown
	}

	// If we've already voted for someone else this term, reject
	if sm.vote != 0 && sm.vote != rv.candidateId {
		if stepDown {
			panic("impossible state in handleRequestVote")
		}
		return VoteResEv{
			term:        sm.term,
			voteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if s.log.lastIndex() > rv.prevLogIndex || s.log.lastTerm() > rv.prevLogTerm {
		return VoteResEv{
			term:        s.term,
			voteGranted: false,
			reason: fmt.Sprintf(
				"our index/term %d/%d > %d/%d",
				sm.log.lastIndex(),
				sm.log.lastTerm(),
				rv.prevLogIndex,
				rv.prevLogTerm,
			),
		}, stepDown
	}

	// We passed all the tests: cast vote in favor
	s.vote = rv.candidateId
	//s.resetElectionTimeout()
	return VoteResEv{
		term:        s.term,
		voteGranted: true,
	}, stepDown
}

func (sm *StateMachine) handleResponseVote(rv VoteResEv) {
				// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if rv.term > sm.term {
				sm.leader = unknownLeader
				sm.state = follower
				sm.vote = noVote
				return 
			}
			if rv.term < sm.term {
				break
			}
			if rv.voteGranted {
				votes[t.id] = true
			}
			// "Once a candidate wins an election, it becomes leader."
			if s.config.pass(votes) {
				s.logGeneric("I won the election")
				s.leader = s.id
				s.state.Set(leader)
				s.vote = noVote
				return // win
			}
}