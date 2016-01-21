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

// Test read , write , cas and delete with multiple clients
func TestMultipleClient(t *testing.T) {
	
	conn := CreateConnections(t,4)
	name := "hi.txt"
    contents := "bye"
    exptime := 300000
    
    var err error
    // get scanner 
    fmt.Println("I am here")
    scanner := make([]*bufio.Scanner, 4)
    for i:=0;i<4;i++ {
    	scanner[i] = bufio.NewScanner(conn[i])
    }

    fmt.Printf("ooohhhhhhh")
    // Write a file
    _,err = fmt.Fprintf(conn[0], "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
    if err !=nil {
            fmt.Printf("error in writing in buffer\n")
    }
    scanner[0].Scan() // read first line
    resp := scanner[0].Text() // extract the text from the buffer
    version := VerifyWriteSucess(t,resp)

    fmt.Printf("2")

    // read from the second client
    fmt.Fprintf(conn[1], "read %v\r\n", name)
    scanner[1].Scan()
    output := scanner[1].Text()
    scanner[1].Scan()     
    VerifyReadSucess(t,output,version,contents,scanner[1].Text())

    fmt.Printf("3")

    // cas from the third client
    contents = "GO is for distributed computing"
    fmt.Fprintf(conn[2], "cas %v %v %v %v\r\n%v\r\n",name,version,len(contents),exptime,contents)
    scanner[2].Scan() // read first line
    version = VerifyCasSucess(t,scanner[2].Text())

    fmt.Printf("4")

    // delete file from third client
    fmt.Fprintf(conn[3],"delete %v\r\n",name)
    scanner[3].Scan()
    VerifyDeleteSucess(t,scanner[3].Text())

    fmt.Printf("4")

    // delete file from third client
    fmt.Fprintf(conn[1],"delete %v\r\n",name)
    scanner[1].Scan()
    VerifyFileNotFound(t,scanner[1].Text())


}

//--------------------------------------- Start of Concurrency Testcases----------------------------------------------------------

// generatl method for performaing cas operation
func PerformCas(version int64,name string,conn net.Conn,dataChannel chan int) {
	
	contents := "hello"
	fmt.Fprintf(conn, "cas %v %v %v\r\n%v\r\n",name,version,len(contents),contents)
	scanner := bufio.NewScanner(conn)

	scanner.Scan()
	output := scanner.Text()
	arr := strings.Split(output," ")
	if arr[0] == "ERR_VERSION" {
			dataChannel <- 0
	} else {
			dataChannel <- 1
	}
}

// If more than one client change the same version , only one of them should succeed
func TestMultiClientCasSingleUpdate(t *testing.T) {

	dataChannel := make(chan int)

	conn := CreateConnections(t,10)
	name := "hi.txt"
    contents := "hello 1"
    scanner := bufio.NewScanner(conn[0])
	_,err := fmt.Fprintf(conn[0], "write %v %v\r\n%v\r\n", name, len(contents) ,contents)
	if err !=nil {
		t.Error(err.Error())
	}
	scanner.Scan()
	version := VerifyWriteSucess(t,scanner.Text())

	for i:=0;i<10;i++ {
		contents = "hello " + strconv.Itoa(i+2);
		go PerformCas(version,name,conn[i],dataChannel)
	}
	// verify with read
	var b int = 0
    for i := 0; i < 10; i++ {
        b = b +  <-dataChannel
    }
    if b!=1 {
    	t.Error(fmt.Sprintf("More than one client has chabged version %v of file %v",version,name)) // t.Error is visible when running `go test -verbose`
    }
}

func PerformMultiCas(t *testing.T,version int64,name string,conn net.Conn,dataChannel chan int64) {

	var err error
	contents := "hello"
	for {
			fmt.Fprintf(conn, "cas %v %v %v\r\n%v\r\n",name,version,len(contents),contents)
			scanner := bufio.NewScanner(conn)

			scanner.Scan()
			output := scanner.Text()
			arr := strings.Split(output," ")
			version,err = strconv.ParseInt(arr[1],10,64)
			if err != nil {
				t.Error(err.Error())
			}

			if arr[0] == "ERR_VERSION" {
					continue
			} else {
					dataChannel <- version
					break
			}
		}
}
/*

func TestMultiClientCasFinalVersion(t *testing.T) {

	var err error
	conn := CreateConnections(t,5)
	dataChannel := make(chan int64)
	fmt.Printf("1")

	name := "hi.txt"
    contents := "hello 1"
    scanner := bufio.NewScanner(conn[0])
    fmt.Printf("haha")

	_,err = fmt.Fprintf(conn[0], "write %v %v\r\n%v\r\n", name, len(contents) ,contents)
	fmt.Printf("jaja")
	if err != nil {
		t.Error(err.Error())
	}

	fmt.Printf("2")
	scanner.Scan()
	version := VerifyWriteSucess(t,scanner.Text())

	for i:=0;i<5;i++ {
		go PerformMultiCas(t,version,name,conn[i],dataChannel)
	}

	arr := make([]int64,10)
	for i:=0;i<10;i++ {
		arr[i] = <-dataChannel
	}
	fmt.Printf("3")
	// read from the second client
    fmt.Fprintf(conn[1], "read %v\r\n", name)
    scanner = bufio.NewScanner(conn[1])
    scanner.Scan()
    output := scanner.Text()
    arr1 := strings.Split(output," ")
    scanner.Scan()
    version,err = strconv.ParseInt(arr1[1],10,64)
    if err != nil {
    	t.Error(err.Error())
    }
    fmt.Printf("4")
    var result bool = false

    for i:=0;i<10;i++ {
    	if version == arr[i] {
    		result = true
    		break
    	}
    }

   	if !result {
    	t.Error("Version error while writing multiple clients")
    }

}
*/

func writeFileTest(t *testing.T,conn net.Conn,dataChannel chan int64,name string) {
    contents := "hello 1"
    scanner := bufio.NewScanner(conn)
	_,err := fmt.Fprintf(conn, "write %v %v\r\n%v\r\n", name, len(contents) ,contents)
	if err !=nil {
		t.Error(err.Error())
	}
	scanner.Scan()
	version := VerifyWriteSucess(t,scanner.Text())

	dataChannel <- version

}


func TestWritewithSameFileName(t *testing.T) {
	name := "hi.txt"
	dataChannel := make(chan int64)
	conn := CreateConnections(t,10)

	for i:=0; i<10;i++ {
		go writeFileTest(t,conn[i],dataChannel,name)
	}

	arr := make([]int64,10)
	for i:=0;i<10;i++ {
		arr[i] = <-dataChannel
	}
	
	// read from the second client
    fmt.Fprintf(conn[1], "read %v\r\n", name)
    scanner := bufio.NewScanner(conn[1])
    scanner.Scan()
    output := scanner.Text()
    arr1 := strings.Split(output," ")
    scanner.Scan()
    version,err := strconv.ParseInt(arr1[1],10,64)
    if err != nil {
    	t.Error(err.Error())
    }
    
    var result bool = false

    for i:=0;i<10;i++ {
    	if version == arr[i] {
    		result = true
    		break
    	}
    }
    
    if !result {
    	t.Error("Version error while writing multiple clients")
    }

}



// Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}

