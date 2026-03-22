package indexer

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

var (
	startTime = time.Now()

	processedMsgs int64
	successMsgs   int64
	failedMsgs    int64
	retriedMsgs   int64

	indexLatencyNs int64
	indexCount     int64

	dbLatencyNs int64
	dbCount     int64
)

func IncrementProcessed() {
	atomic.AddInt64(&processedMsgs, 1)
}

func IncrementSuccess() {
	atomic.AddInt64(&successMsgs, 1)
}

func IncrementFailure() {
	atomic.AddInt64(&failedMsgs, 1)
}

func IncrementRetry() {
	atomic.AddInt64(&retriedMsgs, 1)
}

func AddIndexLatency(d time.Duration) {
	atomic.AddInt64(&indexLatencyNs, d.Nanoseconds())
	atomic.AddInt64(&indexCount, 1)
}

func AddDBLatency(d time.Duration) {
	atomic.AddInt64(&dbLatencyNs, d.Nanoseconds())
	atomic.AddInt64(&dbCount, 1)
}

func humanDuration(d time.Duration) string {
	d = d.Round(time.Second)

	h := d / time.Hour
	d -= h * time.Hour

	m := d / time.Minute
	d -= m * time.Minute

	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func FinalReport() {
	elapsed := time.Since(startTime)

	processed := atomic.LoadInt64(&processedMsgs)
	success := atomic.LoadInt64(&successMsgs)
	failed := atomic.LoadInt64(&failedMsgs)
	retried := atomic.LoadInt64(&retriedMsgs)

	indexLat := atomic.LoadInt64(&indexLatencyNs)
	indexCnt := atomic.LoadInt64(&indexCount)

	dbLat := atomic.LoadInt64(&dbLatencyNs)
	dbCnt := atomic.LoadInt64(&dbCount)

	pps := float64(processed) / elapsed.Seconds()

	var avgIndex float64
	if indexCnt > 0 {
		avgIndex = float64(indexLat/indexCnt) / 1e6
	}

	var avgDB float64
	if dbCnt > 0 {
		avgDB = float64(dbLat/dbCnt) / 1e6
	}

	log.Println("--------------------------------------------------")
	log.Println("Indexer Summary")
	log.Println("--------------------------------------------------")

	log.Printf("Elapsed time: %s", humanDuration(elapsed))
	log.Printf("%d messages processed", processed)
	log.Printf("%d successful", success)
	log.Printf("%d failed", failed)
	log.Printf("%d retried", retried)
	log.Printf("%.2f msgs/sec", pps)
	log.Printf("%.2f ms avg indexing time", avgIndex)
	log.Printf("%.2f ms avg DB time", avgDB)

	log.Println("--------------------------------------------------")
}
