package main

import "fmt"

// Constant represents the differenet state of the node
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

type TimeoutEv struct {
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
	peerid int
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

func (sm *StateMachine) ProcessEvent(ev interface{}) {
	switch ev.(type) {
	
	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)
		// "While waiting for votes, a candidate may receive an
		// appendEntries RPC from another server claiming to be leader.
		// If the leader's term (included in its RPC) is at least as
		// large as the candidate's current term, then the candidate
		// recognizes the leader as legitimate and steps down, meaning
		// that it returns to follower state."
		_, stepDown := sm.handleAppendEntries(cmd)
		if stepDown {
			sm.leader = cmd.leaderId
			sm.state = follower
			return
		}	
		fmt.Printf("%v\n", cmd)
	case AppendEntriesResEv:
		cmd := ev.(AppendEntriesResEv)
		fmt.Printf("%v\n",cmd)
	case VoteReqEv:
		cmd := ev.(VoteReqEv)
		// do stuff with req
		fmt.Printf("%v\n", cmd)
	case VoteResEv:
		cmd := ev.(VoteResEv)
		fmt.Printf("%v\n",cmd)
	case TimeoutEv:
		cmd := ev.(TimeoutEv)
		sm.handleTimeout()
		fmt.Printf("%v\n", cmd)
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
			reason:  fmt.Sprintf("Term %d < %d", r.term, sm.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.term > sm.term {
		sm.term = r.term
		sm.voteFor = noVote
		stepDown = true
	}

	// Special case for candidates: "While waiting for votes, a candidate may
	// receive an appendEntries RPC from another server claiming to be leader.
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and steps down, meaning that it returns to follower state."
	if sm.state == candidate && r.leaderId != sm.leader && r.term >= sm.term {
		sm.term = r.term
		sm.voteFor = noVote
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
		if err := sm.log.appendlogEntry(entry); err != nil {
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
		if err := sm.log.commitTo(r.commitIndex); err != nil {
			return AppendEntriesResEv{
				term:    sm.term,
				success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.commitIndex, err),
			}, stepDown
		}
	}

	// all good
	return AppendEntriesResEv{
		term:    sm.term,
		success: true,
	}, stepDown
}

func (sm *StateMachine) handleAppendEntriesRes() {

}


// handleRequestVote will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=unknownLeader, s.state.Set(Follower).
func (sm *StateMachine) handleRequestVote(rv VoteReqEv) (VoteResEv, bool) {

	// If the request is from an old term, reject
	if rv.term < sm.term {
		return VoteResEv{
			term:        sm.term,
			voteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", rv.term, sm.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if rv.term > sm.term {
		sm.term = rv.term
		sm.voteFor = noVote
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
	if sm.voteFor != 0 && sm.voteFor != rv.candidateId {
		if stepDown {
			panic("impossible state in handleRequestVote")
		}
		return VoteResEv{
			term:        sm.term,
			voteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", sm.voteFor),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if sm.log.lastIndex() > rv.prevLogIndex || sm.log.lastTerm() > rv.prevLogTerm {
		return VoteResEv{
			term:        sm.term,
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
	sm.voteFor = rv.candidateId
	//s.resetElectionTimeout()
	return VoteResEv{
		term:        sm.term,
		voteGranted: true,
	}, stepDown
}

func (sm *StateMachine) pass(votes []bool) (bool){
	count :=0
	for i:=0;i<len(votes);i++ {
		if votes[i] {
			count++
		}
	}
	if(count > len(votes)/2) {
		return true
	} else {
		return false
	}

}

func (sm *StateMachine) handleResponseVote(rv VoteResEv,votes []bool) {
				// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if rv.term > sm.term {
				sm.leader = unknownLeader
				sm.state = follower
				sm.voteFor = noVote
				return 
			}
			if rv.term < sm.term {
				return
			}
			if rv.voteGranted {
				votes[rv.peerid] = true
			}
			// "Once a candidate wins an election, it becomes leader."
			if sm.pass(votes) {
				fmt.Printf("I won the election")
				sm.leader = sm.serverId
				sm.state = leader
				sm.voteFor = noVote
				return // win
			}
}

func (sm *StateMachine) handleTimeout() {
	if sm.state == follower {
			sm.term++
			sm.voteFor = noVote
			sm.leader = unknownLeader
			sm.state = candidate
	}
	if sm.state == candidate {
			sm.term++
			sm.voteFor = noVote
	}

}