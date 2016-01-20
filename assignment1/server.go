package main

// importing various packages
import (
"net"
"os"
"fmt"
"sync"
"strings"
"strconv"
"time"
)

// Data Structure for keeping file information
type FileInfo struct {
	name string
	version int64
	numberofbytes int64
	expiry int64
	isExpiry bool
	updateTime time.Time
	content []byte
}

// Map for keeping the information for each file
var FileStructureLock sync.RWMutex
var FileInfoMap map[string]*FileInfo

 // Server listen on port 8080 and call handleClient() method for each connected client
func serverMain() {

	FileInfoMap = make(map[string]*FileInfo)
	ln, err := net.Listen("tcp",":8080")
	checkError(err) // check for error
	for {
		conn, err := ln.Accept()
		// if there is any error, leave the current client and start again listening on port
		if err != nil {
			fmt.Printf(err.Error())
			continue
		}
		go handleClient(conn); // start communicating for connected client
	}
}

// Start point of the server
func main() {

	serverMain() // server Main function
}

func handleClient(conn net.Conn) {
	const BUFFER_SIZE = 1024 // default buffer size
	const MAX_COMMAND_LENGTH = 400
	const MIN_COMMAND_LENGTH = 5

	defer conn.Close() // once method is executed close the connection

	fmt.Println("Client Connected from", conn.RemoteAddr())

	buf := make([]byte, BUFFER_SIZE)
	commandbuf := make([]byte, BUFFER_SIZE)

	var bufCurrPos int = 0
	var numberofbytes int = 0
	var commandbufCurrPos int = 0
	var commandLen int64 = 0
	var preByte byte = 'd'

	for {
		currByte := readByte(conn,buf,&numberofbytes,&bufCurrPos)
		//fmt.Printf("%c",currByte)
		if preByte == '\r' && currByte == '\n' {
			if commandLen < MIN_COMMAND_LENGTH || commandLen > MAX_COMMAND_LENGTH {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				commandbufCurrPos = 0
				commandLen = 0
				preByte = 'd'
				continue
			}

			commandType,commandArray := parseCommand(string(commandbuf[0:commandbufCurrPos-1]))

			switch {
					case commandType==1:
						//fmt.Printf("write Command\n")
						writeFile(commandArray,buf,&bufCurrPos,&numberofbytes,conn)
					case commandType==2:
						//fmt.Printf("read Command")
						readFile(commandArray,conn)
					case commandType==3:
						//fmt.Printf("cas Command")
						casFile(commandArray,buf,&bufCurrPos,&numberofbytes, conn)
					case commandType == 4:
						//fmt.Printf("delete Command");
						deleteFile(commandArray,conn)
					default:
						conn.Write([]byte("ERR_CMD_ERR\r\n"))
					}
			commandbufCurrPos = 0
			commandLen = 0
			preByte = 'd'
			continue		
		}
		if commandbufCurrPos == BUFFER_SIZE {
			commandbufCurrPos = 0
		}
		//fmt.Printf("%c\n",currByte)
		//fmt.Printf("%d\n",commandbufCurrPos)
		commandbuf[commandbufCurrPos] = currByte
		commandbufCurrPos++
		commandLen++
		preByte = currByte
	}
}

// return a single byte after reading from buffer
func readByte(conn net.Conn,buf []byte,numberofbytes *int,bufCurrPos *int) (byte){
	if *bufCurrPos == *numberofbytes {
		for {
				*bufCurrPos = 0
				*numberofbytes,_ = conn.Read(buf)
				if *numberofbytes ==0 {
				continue
			} else {
				break
			}
		}
	}
	b := buf[*bufCurrPos]
	*bufCurrPos++
	return b
}
/*
0- wrong command
1-write
2-read
3-cas
4-delete
*/
func parseCommand(command string) (int64,[]string) {

	commandArray := strings.Split(command," ")
	if len(commandArray)<=1 {
		return 0,nil
	}

	switch {
		case strings.EqualFold(commandArray[0],"write") && (len(commandArray)==3 || len(commandArray)==4) && len(commandArray[1])<250:
			return 1,commandArray
		case strings.EqualFold(commandArray[0],"read") && len(commandArray)==2:
			return 2,commandArray
		case strings.EqualFold(commandArray[0],"cas") && (len(commandArray)==4 || len(commandArray)==5):
			return 3,commandArray
		case strings.EqualFold(commandArray[0],"delete") && len(commandArray)==2:
			return 4,commandArray
		default:
			return 0,commandArray
	}
}

// This function is executed when client enter the write command
func writeFile(commandArray []string,buf []byte,bufCurrPos *int,numberofbytes *int,conn net.Conn) {
	
	fmt.Printf("I am in write\n")
	var isExpiry bool = false
	var expiry int64
	var currVersion int64
	fileName := commandArray[1]
	fileSize,_ := strconv.ParseInt(commandArray[2],10,64)
	if len(commandArray) == 4 {
		expiry,_ = strconv.ParseInt(commandArray[3],10,64)
		isExpiry = true
	}
	newFileBuffer := make([]byte,fileSize,fileSize)

	for i := int64(0) ; i < fileSize ; i++ {
		newFileBuffer[i] = readByte(conn,buf,numberofbytes,bufCurrPos)
	}
	preByte := readByte(conn,buf,numberofbytes,bufCurrPos)
	currByte := readByte(conn,buf,numberofbytes,bufCurrPos)

	if preByte == '\r' && currByte == '\n' {
		FileStructureLock.Lock()
		f,ok := FileInfoMap[fileName]
		if ok {
				f.numberofbytes = fileSize
				f.updateTime = time.Now()
				if isExpiry {
					f.expiry = expiry
				}
				f.content = newFileBuffer
				currVersion = f.version
			} else {
				var f *FileInfo
				f = new(FileInfo)
				f.numberofbytes = fileSize
				f.updateTime = time.Now()
				if isExpiry {
					f.expiry = expiry
					f.isExpiry = isExpiry
				}
				f.content = newFileBuffer
				f.version = 1
				FileInfoMap[fileName] = f
				currVersion = 1
			}
			FileStructureLock.Unlock()
			conn.Write([]byte("OK "+strconv.FormatInt(currVersion,10)+"\r\n"))
	} else {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
	}
}

// Thid function is execute when client enter the read file command
func readFile(commandArray []string,conn net.Conn) {

	fileName := commandArray[1]

	FileStructureLock.RLock()
	f,ok := FileInfoMap[fileName]
	if !ok {
			FileStructureLock.RUnlock()
			conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
			return		
		}	
	result,remExpiry := isFileExpired(f.isExpiry,f.updateTime,f.expiry)

	if result {
			FileStructureLock.RUnlock()
			conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
			return
	}
	reader := f.content
	conn.Write([]byte("CONTENTS "+strconv.FormatInt(f.version,10)+" "+strconv.FormatInt(f.numberofbytes,10)))
	if(f.isExpiry) {
			conn.Write([]byte(" "+strconv.FormatInt(remExpiry,10)+"\r\n"))
	} else {
			conn.Write([]byte("\r\n"))
	}
	FileStructureLock.RUnlock()
	conn.Write(reader)
	conn.Write([]byte("\r\n"))
}

// Thid function is executed when client enter the delete file command
func deleteFile(commandArray []string,conn net.Conn) {

	fileName := commandArray[1]
	FileStructureLock.Lock()
	f,ok := FileInfoMap[fileName]

	if !ok {
		FileStructureLock.Unlock()
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		return
	}

	result,_ := isFileExpired(f.isExpiry,f.updateTime,f.expiry)
	if result {
			FileStructureLock.Unlock()
			conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
			return
	}

	f.content = nil
	delete(FileInfoMap, fileName)
	conn.Write([]byte("OK\r\n"))
	FileStructureLock.Unlock()
}

// This function is executed when client enter the cas command
func casFile(commandArray []string,buf []byte,bufCurrPos *int,numberofbytes *int,conn net.Conn) {
		
	var newisExpiry bool = false // set
	var newexpiry int64 // set
	var newVersion int64 // set
	var newfileSize int64 // set

	fileName := commandArray[1]
	newVersion,_ = strconv.ParseInt(commandArray[2],10,64)
	newfileSize,_ = strconv.ParseInt(commandArray[3],10,64)
	if len(commandArray) == 5 {
		newexpiry,_ = strconv.ParseInt(commandArray[4],10,64)
		newisExpiry = true
	}

	FileStructureLock.Lock()
	f,status := FileInfoMap[fileName]

	if !status {
		FileStructureLock.Unlock()
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		return
	}

	if f.version != newVersion {
		FileStructureLock.Unlock()
		conn.Write([]byte("ERR_VERSION\r\n"))
		return
	}

	result,_ := isFileExpired(f.isExpiry,f.updateTime,f.expiry)
	if result {
			FileStructureLock.Unlock()
			conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
			return
	}

	newFileBuffer := make([]byte,newfileSize,newfileSize)

	for i := int64(0) ; i < newfileSize ; i++ {
		newFileBuffer[i] = readByte(conn,buf,numberofbytes,bufCurrPos)
	}
	preByte := readByte(conn,buf,numberofbytes,bufCurrPos)
	currByte := readByte(conn,buf,numberofbytes,bufCurrPos)

	if preByte == '\r' && currByte == '\n' {
		f,_ := FileInfoMap[fileName]
		f.numberofbytes = newfileSize
		if newisExpiry {
			f.expiry = newexpiry
			f.isExpiry = true
		}
		f.updateTime = time.Now()
		f.content = newFileBuffer
		f.version = f.version+1
		conn.Write([]byte("OK "+strconv.FormatInt(f.version,10)+" \r\n"))		
		FileStructureLock.Unlock()
	} else {
		FileStructureLock.Unlock()
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
	}
}

func isFileExpired(isExpiry bool,t time.Time,expiry int64) (bool,int64) {
	if !isExpiry {
		return false,0
	}
	elapsedDuration := int64(time.Since(t).Seconds())
	if elapsedDuration > expiry {
		return true,0
	} else {
		return false,expiry - elapsedDuration
	}
}

// This method handles the error and terminate the server process
func checkError(err error) {
    if err != nil {
        fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
        os.Exit(1)
    }
}