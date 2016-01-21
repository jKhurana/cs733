# cs733
Engineering the Cloud

Copyright (c) JATIN KHURANA, All right reserved.

This package contains FILE SERVER which manages files. This package manages 4 operations on file
1- write
2- read
3- cas (compare and swap)
4- delete

Follwoing is the command for each operation.

WRITE:
write <filename> <numberofbytes> [<exptime>]\r\n
contentoffile\r\n

READ:
read filename\r\n

COMPARE AND SWAP:
cas <filename> <version> <numberofbytes> [<exptime>]\r\n
contentoffile\r\n

DELETE:
delete <filename>

----------------------------QUICKSTART--------------------------------

First set the variable GPPATH and GOBIN and put the cs733 folder inside <GOPATH>

Install the server with the following command

>go install $GOPATH\cs733\assignment1

Now executable file would be available in <GOBIN>

Go to bin folder and run the server

>./server

Now connect the client and perform the above operation

------------------------------------------Specification and Assumption---------------------------------------
1- Server run on port 8080.
2- Server stoers the file in memory only so when we close the server all data are lost.
3. Maximum number of bytes in a file should be 1000000000



-------------------------------------------------Testing---------------------------
There is a seperate test file with the name "basic_test.go" which contains a lot of test cases.

In order to run these test case, run the following command

> go test $GOPATH/cs733/assignment1

It will output "OK" which means all the test cases are executed successfully



