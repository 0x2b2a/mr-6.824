package mr

import "fmt"

const debugEnabled = false

const (
	MapPhase    = 001
	ReducePhase = 002
)

type Task struct {
	fileName string
	nReduce int
	nMaps int
	Seq int
	phase int
	alive bool
}

func info(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func max(value1 int, value2 int) int {
	if value1 > value2 {
		return value1
	} else {
		return value2
	}
}