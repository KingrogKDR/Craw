package parsing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KingrogKDR/Dev-Search/queues"
	"github.com/google/uuid"
)

const (
	ParsingTimeout = 20 * time.Second
	ParseQueueKey  = "parsemeta"
)

type ParsingWorker struct {
	ID      string
	Payload string
	parseQ  *queues.Queue

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	concurrency int
	timeout     time.Duration
}

func NewParsingWorker(payload string, parseQ *queues.Queue, concurrency int) *ParsingWorker {
	ctx, cancel := context.WithCancel(context.Background())
	workerID := fmt.Sprintf("%s-%s", "Parsing", uuid.New().String())
	return &ParsingWorker{
		ID:          workerID,
		Payload:     payload,
		parseQ:      parseQ,
		ctx:         ctx,
		cancel:      cancel,
		concurrency: concurrency,
		timeout:     ParsingTimeout,
	}
}
