package main

import (
	"time"

	"github.com/KingrogKDR/Dev-Search/queues"
	"github.com/KingrogKDR/Dev-Search/storage"
)

var Queues = []string{
	string(queues.P0_CRITICAL),
	string(queues.P1_HIGH),
	string(queues.P2_NORMAL),
	string(queues.P3_LOW),
}

func main() {
	rdb := storage.GetRedisClient()
	frontier := queues.NewQueue(rdb, "frontier")
	job := queues.NewJob("example.com")
	frontier.Enqueue(job)
	frontier.Dequeue(Queues, "1", 5*time.Second)
}
