package main

import (
"io"
"encoding/binary"
)

// various constants
const (
logEntryHeaderSize = 24 // Size of the header of the log entry (must be changed if header of the log changed)
)

//log structure of a server node
type Log struct {
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
	term int64
	index int64
	command []byte
}

//---------------------------------------------Some general methods---------------------------------------------------------


func (log *Log) lastIndex() uint64 {
	log.RLock()
	defer log.RUnLock()
	if len(log.logRecord) <=0 {
		return 0
	}
	lastLogEntry := log.logRecord[len(log.logRecord)-1]
	return lastLogEntry.index
}

func (log *Log) lastTerm() uint64 {
	log.RLock()
	defer log.RUnLock()
	if len(log.logRecord) <=0 {
		return 0
	}
	lastLogEntry := log.logRecord[len(log.logRecord)-1]
	return lastLogEntry.term
}

//--------------------------------------------------------------------------------------------------------------------------

// thos method create the new log and return its address
// this method is called when a new node is up inititally
func createNewLog(logReadWrite io.ReadWriter) *Log {
	log := &Log{
		logRecord:	[]logEntry{}
		commitPosition:	-1 // no commit entry in the starting position
		logReadWrite:	logReadWrite
	}
	return log
}

/*
This method append the log entry into the log of the server
Because term and Index monotonically increasing in the log of a server So this method throws an error when 
lastTerm or lastIndex of log is greater than the term and Index of given log entry respectively
*/
func (log *Log) appendlogEntry(entry logEntry) error {
	
	log.Lock()
	defer log.UnLock()

	lastLogIndex := log.lastIndex()
	if lastLogIndex > entry.index {
		return error.New("Index valud is too small")
	}
	lastLogTerm := log.lastTerm()
	if lastLogTerm > entry.term {
		return error.New("Term valud is too small")
	}

	log.logRecord = append(log.logRecord,entry)
	return nil
}

// this method read the log entry from the disk 
func (le *logEntry) readLogEntry(reader io.Reader) error{

	buf := make([]byte,logEntryHeaderSize)
	
	_ err := reader.Read(buf)
	if err != nil {
		return err
	}

	command := make([]byte,binary.LittleEndian.Uint64(buf[16:24]))
	_ err = reader.Read(command)
	if err != nil {
		return err
	}

	e.term = binary.LittleEndian.Uint64(buf[0:8])
	e.index = binary.LittleEndian.Uint64(buf[8:16])
	e..command = command

}

// this mehtod write the log entry into the disk
func (le *logEntry) writeLogEntry(w io.Writer) error {

	commandLen := len(le.command)
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




