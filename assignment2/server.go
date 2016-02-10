package main

import (
"fmt"
"sync"
"io"
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

// protected variables to maintain the consistency

// Protected Int variable
type protectedInt struct {
	sync.RWMutex
	value int
}

func (i *protectedInt) setInt64(value int) {
	i.Lock()
	defer i.UnLock()
	i.value = value;
}

func (i *protectedInt) getInt() bool {
	i.Lock()
	defer i.UnLock()
	return i.value
}

type protectedBool struct {
	sync.RWMutex
	value bool
}

func (b *protectedBool) setBool(value bool) {
	b.Lock()
	defer b.UnLock()
	b.value = value;
}

func (i *protectedInt) getBool() bool {
	b.Lock()
	defer b.UnLock()
	return b.value
}

// data structure for the indivisual server
type Server struct {

	serverId uint64 // unique server id of the server
	state *protectedInt // current state of the server
	runningStatus *protectedBool // whether server is running or not
	term uint64 // current term number of the server
	vote uint64 // if applicable id of the server to whom server voted for the curreent term 
	log *Log // log of the server
	
	appendEntriesChannel chan appendEntriesReqRes // this require for sending the request for appendEntries or receiving its response
	voteChannel chan voteReqRes // require for sending vote request and receiving its response
}


// this method create a new server with the given id and readWriter. This method assumes that given 
// id is unique among the all created or running servers
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
		term:	latestTerm,
		vote:	-1,
		log:	log,
		appendEntriesChannel:	make(chan appendEntriesReqRes)
		voteChannel:	make(chan voteReqRes)
	}
}

// This method run the server continusously until runningStatus is true
func (s *Server) run() {
	s.runningStatus.setBool(true)

	for s.runningStatus.getBool() {
		currentState := s.state.getInt()
		switch currentState {
		case 1:
			s.handleFolloweState()
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
func (s *Server) handleFolloweState() {

}

// this method handle the leader state
func (s *Server) handleLeaderState() {

}

// this method handle the candidate state
func (s *Server) handleCandidateState() {

}

//-------------------------------------------------------------------------------------END-----------------------------------------------------------------

// this function start the server
func (s *Server) start() {
	go s.run();
}

// this function stop the server
func (s *Server) stop() {
	s.runningStatus.set(false)
}

