package main

import (
        "bufio"
        "fmt"
        "net"
        "strconv"
        "strings"
        "testing"
        "time"
)

// ----------------------------------Methods to verify the actural output with Expected Output----------------------------------------

func VerifyReadSucess(t *testing.T,output string,version int64,contents string,outputcontnets string) {
	arr := strings.Split(output, " ")
	expect(t, arr[0], "CONTENTS")
    expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
    expect(t, arr[2], fmt.Sprintf("%v", len(contents)))
    expect(t, contents, outputcontnets)
}

func VerifyWriteSucess(t *testing.T,output string) (int64){
	arr := strings.Split(output, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version,err := strconv.ParseInt(arr[1],10,64)
	if err!=nil {
		t.Error("Version is not numeric")
	}
	return version
}

func VerifyDeleteSucess(t *testing.T,output string) {
	expect(t,output,"OK");
}

func VerifyCasSucess(t *testing.T,output string) (int64) {
	arr := strings.Split(output, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version,err := strconv.ParseInt(arr[1],10,64)
	if err!=nil {
		t.Error("Version is not numeric")
	}
	return version
}

func VerifyCasVersionError(t *testing.T,output string) (int64){
	arr := strings.Split(output, " ") // split into OK and <version>
	expect(t, arr[0], "ERR_VERSION")
	version,err := strconv.ParseInt(arr[1],10,64)
	if err!=nil {
		t.Error("Version is not numeric")
	}
	return version
}

func VerifyFileNotFound(t *testing.T,output string) {
	expect(t,output,"ERR_FILE_NOT_FOUND")
}

//-------------------------------------------------------------------------------------------------------------------------


//-------------------------------------------------General Utility Methods--------------------------------------------------------


func CreateConnections(t *testing.T,count int) ([]net.Conn) {	
	var err error
	conn := make([]net.Conn, count)
	for i:=0; i<count; i++ {
		conn[i],_ = net.Dial("tcp","localhost:8080")
		if err !=nil {
			t.Error(err.Error())
			break
		}
	}
	return conn
}

//-------------------------------------------------------------------------------------------------------------------------------


// ---------------------------------------------------------------Start Test Cases--------------------------------------------------

// Simple serial check of read and write
func TestTCPSimple(t *testing.T) {
        go serverMain()
        name := "hi.txt"
        contents := "bye"
        exptime := 300000
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)

        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        version := VerifyWriteSucess(t,resp)

        // try read now
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()
        output := scanner.Text()
        scanner.Scan()     
        VerifyReadSucess(t,output,version,contents,scanner.Text())
}

// Simple serial check of read , write , cas , delete which also include ERR_FILE_NOT_FOUND condition
func TestSingleClient(t *testing.T) {
        name := "hi.txt"
        contents := "bye"
        exptime := 300000
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)


        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        version := VerifyWriteSucess(t,resp)


        // try read now
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()
        output := scanner.Text()
        scanner.Scan()     
        VerifyReadSucess(t,output,version,contents,scanner.Text())

        // try a cas command
        contents = "GO is for distributed computing"
        fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n",name,version,len(contents),exptime,contents)
        scanner.Scan() // read first line
        version = VerifyCasSucess(t,scanner.Text())


        // try delete
        fmt.Fprintf(conn,"delete %v\r\n",name)
        scanner.Scan()
        VerifyDeleteSucess(t,scanner.Text())

        //try delete file not found
        fmt.Fprintf(conn,"delete %v\r\n",name)
        scanner.Scan()
        VerifyFileNotFound(t,scanner.Text())

        //try read file not found
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()     
        VerifyFileNotFound(t,scanner.Text())
}

//checking serial cas version error
func TestCasVersionError(t *testing.T) {
		name := "hi.txt"
        contents := "bye"
        exptime := 300000
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)


        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        version := VerifyWriteSucess(t,resp)

        // try a cas command
        contents = "GO is for distributed computing"
        fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n",name,version+1,len(contents),exptime,contents)
        scanner.Scan() // read first line
        version = VerifyCasVersionError(t,scanner.Text())
}

// test expiretime
func TestExpireTime(t *testing.T) {
		name := "hi.txt"
        contents := "jatin"
        exptime := 2
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        _ = VerifyWriteSucess(t,resp)

        time.Sleep(5*time.Second)
        //try read file not found
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()     
        VerifyFileNotFound(t,scanner.Text())
}

//--------------------------------------- Start of Concurrency Testcases----------------------------------------------------------
/*
// generatl method for performaing cas operation
func PerformCas(version int64,name string,conn net.Conn) {
	
	contents = "hello " + strconv.PraseInt(version,10)
	fmt.Fprintf(conn[i], "cas %v %v %v %v\r\n%v\r\n",name,version,len(contents),contents)

}

func TestCas(t *testing.T) {

	dataChannel := make(chan int)

	conn := CreateConnections(t,10)
	name := "hi.txt"
    contents := "hello 1"
    scanner := bufio.NewScanner(conn[0])
	_,err := fmt.Fprintf(conn[0], "write %v %v %v\r\n%v\r\n", name, len(contents) ,contents)
	scanner.Scan()
	version := VerifyWriteSucess(t,scanner.Text())

	for i:=0;i<10;i++ {
		contents = "hello " + strconv.Itoa(i+2);
		fmt.Fprintf(conn[i], "cas %v %v %v %v\r\n%v\r\n",name,version,len(contents),exptime,contents)
		version = VerifyCasSucess()
	}
	// verify with read
	fmt.Fprintf(conn[0], "read %v\r\n", name)
	scanner.Scan()
    output := scanner.Text()
    scanner.Scan()     
    VerifyReadSucess(t,output,version,"hello 11",scanner.Text())
}

func TestMultipleClient(t *testing.T) {

}

*/

// Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}

