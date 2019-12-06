package main

// read a file and split it into smal chunks chunks
// This works with go routine

import (
	"fmt"
	"os"
	"strconv"
	"sync"
)

type chunk struct {
	bufsize int
	offset  int64
}

// Create waitgoup to make go main program wait till all the go routines are completed
var wg sync.WaitGroup

func check(e error) {
	if e != nil {
		panic(e)
	}
}

/*
	Write the file in the output location
*/
func writeFile(id int, byteString []byte) {
	filename := fmt.Sprint("./out/part-", strconv.Itoa(id))
	f, err := os.Create(filename)
	check(err)

	defer f.Close()

	n2, err := f.Write(byteString)
	check(err)
	fmt.Printf("wrote %d bytes into %s\n", n2, filename)
	f.Sync()
}

/*
	read the file in defined block size and pass the byte array to the write method
	NOTE :- FOR CONCURENCY THIS METHOD SHOULD PERFORM INSIDE GO ROUTINE
*/
func readFile(chunksizes []chunk, i int, file *os.File, ch chan int) {
	fmt.Printf("Started co-routine : %d\n", i)
	defer wg.Done()

	chunk := chunksizes[i]
	buffer := make([]byte, chunk.bufsize)
	bytesread, err := file.ReadAt(buffer, chunk.offset)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("bytes read: ", bytesread)

	writeFile(i, buffer)

	fmt.Printf("Completed co-routine : %d\n", <-ch)
}

func main() {
	fmt.Printf("File spliting started\n")
	// BufferSize -> Maximum size of output file
	// MaximumNoOfConcurrency -> Maximum number of cncurrent go routine
	BufferSize := 1024 * 1024 * 10
	MaximumNoOfConcurrency := 5
	fmt.Printf("Maximum number of cncurrent write is : %d\n", MaximumNoOfConcurrency)

	// Create channel to limit no of go routines
	ch := make(chan int, MaximumNoOfConcurrency)

	file, err := os.Open("data.csv")
	check(err)

	fileinfo, err := file.Stat()
	check(err)
	filesize := int(fileinfo.Size())
	fmt.Printf("The file is %d bytes long\n", filesize)

	// Number of go routines we need to spawn.
	concurrency := filesize / BufferSize
	fmt.Printf("Number Of Files Created : %d\n", concurrency)

	// buffer sizes that each of the go routine below should use. ReadAt
	// returns an error if the buffer size is larger than the bytes returned
	// from the file.
	chunksizes := make([]chunk, concurrency)

	// All buffer sizes are the same in the normal case. Offsets depend on the
	// index. Second go routine should start at 100, for example, given our
	// buffer size of 100.
	for i := 0; i < concurrency; i++ {
		chunksizes[i].bufsize = BufferSize
		chunksizes[i].offset = int64(BufferSize * i)
	}

	// check for any left over bytes. Add the residual number of bytes as the
	// the last chunk size.
	if remainder := filesize % BufferSize; remainder != 0 {
		c := chunk{bufsize: remainder, offset: int64(concurrency * BufferSize)}
		concurrency++
		chunksizes = append(chunksizes, c)
	}

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		ch <- i
		go readFile(chunksizes, i, file, ch)
	}

	// Wait till all the go routines are completed
	wg.Wait()

	fmt.Printf("File spliting completed\n")
}
