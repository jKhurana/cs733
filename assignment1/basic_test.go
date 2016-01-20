package main

import (
        "bufio"
        "fmt"
        "net"
        "strconv"
        "strings"
        "testing"
)


// Simple serial check of getting and setting
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
        //n,err := conn.Write([]byte("write hi.txt 3000000 6\r\nbye\r\n"))
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        arr := strings.Split(resp, " ") // split into OK and <version>
        expect(t, arr[0], "OK")
        ver, err := strconv.Atoi(arr[1]) // parse version as number
        if err != nil {
                t.Error("Non-numeric version found")
        }
        version := int64(ver)

        fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
        scanner.Scan()
        result := scanner.Text()
        fmt.Printf(result)
        arr = strings.Split(result, " ")
        expect(t, arr[0], "CONTENTS")
        expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
        expect(t, arr[2], fmt.Sprintf("%v", len(contents)))
        expect(t, arr[3], fmt.Sprintf("%v", exptime))
        scanner.Scan()
        expect(t, contents, scanner.Text())
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}
