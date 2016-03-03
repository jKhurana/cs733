 package main

import (
        "fmt"
        "testing"
)

/*
State Machine data structure
type StateMachine struct {
	serverId uint64 // unique server id of the server
	peer []int // peers id
	state uint // current state of the server
	leader uint64 // keep the id of the leader
	term uint64 // current term number of the server
	voteFor uint64 // if applicable id of the server to whom server voted for the curreent term 
	log *serverLog // log of the server
}
//log structure of a server node
type serverLog struct {
	sync.RWMutex
	logRecord []logEntry
	prevLogTerm uint64
	prevLogIndex uint64
	commitPosition int
	logReadWrite io.Writer
}
*/

 func TestWrongTerm(t *testing.T) {
       sm := initiateStateMachine()
       sm.term = 4
       sm.state = follower
       sm.leader = 2
       sm.voteFor = noVote
       sm.ProcessEvent(AppendEntriesReqEv{term : 1, prevLogIndex: 1, prevLogTerm: 2,leaderId: 2,commitIndex: 0})
 }

// this method initiate the state machine with the 4 peer nodes
 func initiateStateMachine() (sm *StateMachine) {
 	var s *StateMachine
 	s = new(StateMachine)
 	s.serverId = 1
 	s.peer = make([]int,4)
 	s.peer[0]=2
 	s.peer[1]=3
 	s.peer[2]=4
 	s.peer[3]=5
 	s.log = &serverLog{}
 	return s
 }


 // Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}