/*
This file consists of the data types which are reqired for the communication among variuos servers
The name "DataContract" has been taken from the WCF datacontract concept.
*/

package main

type requestAppendEntriesDC struct {
	prevLogTerm uint64
	prevLogIndex uint64
	leaderId uint64
	logRecords []logEntry
}

type responseAppendEntriesDC struct {
	term uint64
	success bool
}

type appendEntriesReqRes struct {
	request requestAppendEntriesDC
	response chan responseAppendEntriesDC
}

type requestVoteDC struct {
	term uint64
	candidateId uint64
	prevLogTerm uint64
	prevLogIndex uint64
}

type responseVoteDC struct {
	term uint64
	voteGranted bool
}

type voteReqRes struct {
	request requestVoteDC
	response chan responseVoteDC
}