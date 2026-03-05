package queues

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	FrontierKey   = "frontier:%s"
	ProcessingKey = "processing:%s"
	ResultsKey    = "results:%s"
	FailedKey     = "failed"
	RetryKey      = "retry"

	ProcessingTimeout = 5 * time.Minute
)

type Queue struct {
	redis     *redis.Client
	ctx       context.Context
	namespace string
}

func NewQueue(redisClient *redis.Client, namespace string) *Queue {
	return &Queue{
		redis:     redisClient,
		ctx:       context.Background(),
		namespace: namespace,
	}
}

func (q *Queue) Enqueue(job *Job) error {
	var effectiveScore int64

	if job.LastEnqueuedAt.IsZero() {
		effectiveScore = job.BaseScore
	} else {
		waited := time.Since(job.LastEnqueuedAt)
		effectiveScore = ApplyAging(job.BaseScore, waited)
	}

	job.Priority = ScoreToPriority(effectiveScore)
	job.LastEnqueuedAt = time.Now()

	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("Failed to marshal job: %w", err)
	}

	queueKey := fmt.Sprintf(FrontierKey, string(job.Priority))

	err = q.redis.RPush(q.ctx, queueKey, jobData).Err()
	if err != nil {
		return fmt.Errorf("Failed to enqueue task: %w", err)
	}

	log.Printf("Job %s enqueued to '%s' queue", job.ID, job.Priority)

	return nil
}

func (q *Queue) Dequeue(queues []string, workerID string, timeout time.Duration) (*Job, error) {
	queueKeys := make([]string, len(queues))
	for i, queue := range queues {
		queueKeys[i] = fmt.Sprintf(FrontierKey, queue)
	}

	result, err := q.redis.BLPop(q.ctx, timeout, queueKeys...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // no available jobs
		}
		return nil, fmt.Errorf("Failed to dequeue task: %w", err)
	}

	var job Job

	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal task: %w", err)
	}

	processingKey := fmt.Sprintf(ProcessingKey, workerID)
	jobData := result[1]

	job.Status = JOB_INFLIGHT
	job.VisibilityStart = time.Now()

	pipe := q.redis.Pipeline()
	pipe.LPush(q.ctx, processingKey, jobData)
	pipe.Expire(q.ctx, processingKey, ProcessingTimeout)

	_, err = pipe.Exec(q.ctx)

	if err != nil {
		log.Printf("Warning: failed to move task to processing: %v", err)
	}

	log.Printf("Job for '%s' moved to processing state!", job.URL)

	return &job, nil
}

func (q *Queue) CompleteJob(job *Job, result *Result, workerID string) error {
	processingKey := fmt.Sprintf(ProcessingKey, workerID)
	resultKey := fmt.Sprintf(ResultsKey, job.ID)

	jobData, _ := json.Marshal(job)
	resultData, _ := json.Marshal(result)

	pipe := q.redis.Pipeline()

	pipe.LRem(q.ctx, processingKey, 1, jobData)

	pipe.Set(q.ctx, resultKey, resultData, 24*time.Hour)

	if !result.Success {
		if job.RetryCount > MAX_RETRIES {
			pipe.LPush(q.ctx, FailedKey, jobData)
		} else {
			job.RetryCount++

			job.BaseScore -= 10

			if job.BaseScore < 0 {
				job.BaseScore = 10
			}

			job.Priority = ScoreToPriority(job.BaseScore)

			backoff := time.Duration(DEFAULT_RETRY_DELAY)

			jitter := time.Duration(rand.Int63n(int64(5 * time.Second)))

			delay := backoff + jitter

			retryTime := time.Now().Add(delay).Unix()

			pipe.ZAdd(q.ctx, RetryKey, redis.Z{
				Score:  float64(retryTime),
				Member: jobData,
			})

		}
	}

	_, err := pipe.Exec(q.ctx)
	return err
}

func (q *Queue) ProcessRetryJobs() error {
	now := float64(time.Now().Unix())
	results, err := q.redis.ZRangeByScoreWithScores(q.ctx, RetryKey, &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%f", now),
	}).Result()

	if err != nil {
		return err
	}

	if len(results) == 0 {
		return nil
	}

	pipe := q.redis.Pipeline()

	for _, result := range results {
		jobData := result.Member.(string)

		var job Job
		if err = json.Unmarshal([]byte(jobData), &job); err != nil {
			continue
		}

		waited := time.Since(job.LastEnqueuedAt)
		job.BaseScore = ApplyAging(job.BaseScore, waited)
		job.Priority = ScoreToPriority(job.BaseScore)
		job.LastEnqueuedAt = time.Now()

		newJobData, _ := json.Marshal(job)

		queueKey := fmt.Sprintf(FrontierKey, string(job.Priority))

		pipe.RPush(q.ctx, queueKey, newJobData)

		pipe.ZRem(q.ctx, RetryKey, jobData)
	}

	_, err = pipe.Exec(q.ctx)

	if err != nil {
		return err
	}

	log.Printf("Moved %d retry jobs to queues", len(results))
	return nil
}
