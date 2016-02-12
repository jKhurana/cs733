package main

import (
"io"
"encoding/binary"
"sync"
"errors"
)

// various constants
const (
logEntryHeaderSize = 24 // Size of the header of the log entry (must be changed if header of the log changed)
)

//log structure of a server node
type serverLog struct {
	sync.RWMutex
	logRecord []logEntry
	commitPosition int64
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
type logEntry struct {
	term uint64
	index uint64
	command []byte
}

//---------------------------------------------Some general methods---------------------------------------------------------


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




