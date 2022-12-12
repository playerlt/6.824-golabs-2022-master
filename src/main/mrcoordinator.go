package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"log"
	"my6.824/mr"
)
import "time"
import "os"
import "fmt"

func main() {
	log.SetFlags(0)
	start := time.Now()
	fmt.Println("distribute-start: ", start.Format("2006-01-02 15:04:05"))
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	end := time.Now()
	fmt.Println("distribute-end: ", end.Format("2006-01-02 15:04:05"))
	fmt.Printf("distribute run time: %vms\n", end.UnixMilli()-start.UnixMilli())
	time.Sleep(time.Second)
}
