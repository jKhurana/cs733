package main

import (
"io"
"encoding/binary"
"sync"
"errors"
"fmt"
)

// various constants
const (
logEntryHeaderSize = 24 // Size of the header of the log entry (must be changed if header of the log changed)
)

var (
	errTermTooSmall    = errors.New("term too small")
	errTermTooBig    = errors.New("term too big")
	errIndexTooSmall   = errors.New("index too small")
	errIndexTooBig     = errors.New("commit index too big")
	errNoCommand       = errors.New("no command")
	errBadIndex        = errors.New("bad index")
	errBadTerm         = errors.New("bad term")
)

//log structure of a server node
type serverLog struct {
	sync.RWMutex
	logRecord []logEntry
	commitPosition int
	logReadWrite io.Writer
}

/* Each Log Entry
 Each lof Entry has the following format
 --------------------------------------------------------------------------------------------------------------------------------
 	index  |  term   |  commandSize  |  command
 	uint64 |  uint64 |  uint64		 |   byte[]
 -------------------------------------------------------------------------------------------------------------------------------

 Note: while adding more entry to the log , don't change the position of the entries(always append at last)
*/
 // term and index starts with one
type logEntry struct {
	term uint64
	index uint64
	command []byte
}

//---------------------------------------------Some general methods---------------------------------------------------------


func (l *serverLog) commitIndex() uint64 {
	if l.commitPosition < 0 {
		return 0
	}
	if l.commitPosition >= len(l.logRecord) {
		panic(fmt.Sprintf("commitPosition %d > len(l.logRecord) %d; bad bookkeeping in raftLog", l.commitPosition, len(l.logRecord)))
	}
	return l.logRecord[l.commitPosition].index
}

func (l *serverLog) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	if len(l.logRecord) <=0 {
		return 0
	}
	lastLogEntry := l.logRecord[len(l.logRecord)-1]
	return lastLogEntry.index
}

func (l *serverLog) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	if len(l.logRecord) <=0 {
		return 0
	}
	lastLogEntry := l.logRecord[len(l.logRecord)-1]
	return lastLogEntry.term
}

//--------------------------------------------------------------------------------------------------------------------------

// thos method create the new log and return its address
// this method is called when a new node is up inititally
func createNewLog(logReadWrite io.ReadWriter) *serverLog {
	l := &serverLog{
		logRecord:	[]logEntry{},
		commitPosition:	-1, // no commit entry in the starting position
		logReadWrite:	logReadWrite,
	}
	return l
}

/*
This method append the log entry into the log of the server
Because term and Index monotonically increasing in the log of a server So this method throws an error when 
lastTerm or lastIndex of log is greater than the term and Index of given log entry respectively
*/
func (l *serverLog) appendlogEntry(entry logEntry) error {
	
	l.Lock()
	defer l.Unlock()

	lastLogIndex := l.lastIndex()
	if lastLogIndex > entry.index {
		return errors.New("Index valud is too small")
	}
	lastLogTerm := l.lastTerm()
	if lastLogTerm > entry.term {
		return errors.New("Term valud is too small")
	}

	l.logRecord = append(l.logRecord,entry)
	return nil
}

// this method read the log entry from the disk 
func (le *logEntry) readLogEntry(reader io.Reader) error{

	buf := make([]byte,logEntryHeaderSize)
	
	_,err := reader.Read(buf)
	if err != nil {
		return err
	}

	command := make([]byte,binary.LittleEndian.Uint64(buf[16:24]))
	_,err = reader.Read(command)
	if err != nil {
		return err
	}

	le.term = binary.LittleEndian.Uint64(buf[0:8])
	le.index = binary.LittleEndian.Uint64(buf[8:16])
	le.command = command
	return nil
}

// this mehtod write the log entry into the disk
func (le *logEntry) writeLogEntry(w io.Writer) error {

	commandLen := uint64(len(le.command))
	buf := make([]byte,logEntryHeaderSize+commandLen) // buffer to write the log into persistent storage

	// Put the data into the buffer
	binary.LittleEndian.PutUint64(buf[0:8],le.term)
	binary.LittleEndian.PutUint64(buf[8:16],le.index)
	binary.LittleEndian.PutUint64(buf[16:24],commandLen)

	// copy the actual command into the buffer
	copy(buf[24:],le.command)

	_,err := w.Write(buf)
	return err
}


// ensureLastIs deletes all non-committed log entries after the given index and
// term. It will fail if the given index doesn't exist, has already been
// committed, or doesn't match the given term.
//
// This method satisfies the requirement that a log entry in an AppendEntries
// call precisely follows the accompanying LastraftLogTerm and LastraftLogIndex.
func (l *serverLog) ensureLastIndexandTerm(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	if index < l.commitIndex() {
		return errIndexTooSmall
	}

	if index > l.lastIndex() {
		return errIndexTooBig
	}

	// Normal case: find the position of the matching log entry.
	pos := 0
	for ; pos < len(l.logRecord); pos++ {
		if l.logRecord[pos].index < index {
			continue // didn't find it yet
		}
		if l.logRecord[pos].index > index {
			return errBadIndex // somehow went past it
		}
		if l.logRecord[pos].index != index {
			panic("not <, not >, but somehow !=")
		}
		if l.logRecord[pos].term != term {
			return errBadTerm
		}
		break // good
	}

	// Sanity check.
	if pos < l.commitPosition {
		panic("index >= commitIndex, but pos < commitPos")
	}

	// `pos` is the position of log entry matching index and term.
	// We want to truncate everything after that.
	truncateFrom := pos + 1
	if truncateFrom >= len(l.logRecord) {
		return nil // nothing to truncate
	}

	// Truncate the log.
	l.logRecord = l.logRecord[:truncateFrom]

	// Done.
	return nil
}

// commitTo commits all log entries up to and including the passed commitIndex.
// Commit means: synchronize the log entry to persistent storage, and call the
// state machine apply function for the log entry's command.
func (l *serverLog) commitTo(commitIndex uint64) error {
	if commitIndex == 0 {
		panic("commitTo(0)")
	}

	l.Lock()
	defer l.Unlock()

	// Reject old commit indexes
	if commitIndex < l.commitIndex() {
		return errIndexTooSmall
	}

	// Reject new commit indexes
	if commitIndex > l.lastIndex() {
		return errIndexTooBig
	}

	// If we've already committed to the commitIndex, great!
	if commitIndex == l.commitIndex() {
		return nil
	}

	// We should start committing at precisely the last commitPos + 1
	pos := l.commitPosition + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}

	// Commit logRecord between our existing commit index and the passed index.
	for {

		if pos >= len(l.logRecord) {
			panic(fmt.Sprintf("commitTo pos=%d advanced past all logRecord (%d)", pos, len(l.logRecord)))
		}
		if l.logRecord[pos].index > commitIndex {
			panic("commitTo advanced past the desired commitIndex")
		}

		// Mark our commit position cursor.
		l.commitPosition = pos

		// If that was the last one, we're done.
		if l.logRecord[pos].index == commitIndex {
			break
		}
		if l.logRecord[pos].index > commitIndex {
			panic(fmt.Sprintf(
				"current entry Index %d is beyond our desired commitIndex %d",
				l.logRecord[pos].index,
				commitIndex,
			))
		}

		// Otherwise, advance!
		pos++
	}

	// Done.
	return nil
}




