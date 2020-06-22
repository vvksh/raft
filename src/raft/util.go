package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// returns the next random election timer duration
func getNewElectionTimerDuration() time.Duration {
	newTimeoutMillis := minElectionTimeoutMillis + rand.Intn(electionTimeoutMillisRange)
	return time.Duration(newTimeoutMillis) * time.Millisecond
}

func getCurrentTimeString() string {
	return time.Now().Format("2006-01-02 15:04:05.000")
}
