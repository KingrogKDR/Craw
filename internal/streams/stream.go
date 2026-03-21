package streams

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	MAX_RETRIES = 5
)

type MsgStream struct {
	namespace string
	client    *redis.Client
	ctx       context.Context
}

func NewMsgStream(client *redis.Client, namespace string, consumerGroup string) *MsgStream {
	ms := &MsgStream{
		namespace: namespace,
		client:    client,
		ctx:       context.Background(),
	}

	streamName := fmt.Sprintf("%s-events", namespace)
	groupName := fmt.Sprintf("%s-group", consumerGroup)
	err := ms.client.XGroupCreateMkStream(ms.ctx, streamName, groupName, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		panic(fmt.Errorf("failed to create consumer group: %w", err))
	}

	return ms
}

func (ms *MsgStream) AddMsg(msg *Msg, streamName string) error {
	msg.AddedAt = time.Now()

	msgJson, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("Can't marshal message in stream: %w", err)
	}
	ms.client.XAdd(ms.ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]any{
			"data": msgJson,
		},
	})

	return nil
}

func (ms *MsgStream) GetMsg(consumer string, streamName string, groupName string) ([]Msg, error) {
	read := func(id string) ([]redis.XStream, error) {
		return ms.client.XReadGroup(ms.ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumer,
			Streams:  []string{streamName, id},
			Count:    10,
			Block:    0,
		}).Result()
	}

	var streams []redis.XStream

	// try pending msgs first

	streams, err := read("0")
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("Can't read from stream: %w", err)
	}

	// if not pending -> try new
	if len(streams) == 0 {
		streams, err = read(">")
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("Can't read from stream: %w", err)
		}
	}

	// if still empty -> try autoclaim
	if len(streams) == 0 {
		res, _, err := ms.client.XAutoClaim(ms.ctx, &redis.XAutoClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumer,
			MinIdle:  time.Minute,
			Start:    "0",
			Count:    10,
		}).Result()

		if err != nil && err != redis.Nil {
			return nil, err
		}

		// convert claimed messages into XStream-like structure
		if len(res) > 0 {
			streams = []redis.XStream{
				{
					Stream:   streamName,
					Messages: res,
				},
			}
		}
	}

	var result []Msg

	for _, stream := range streams {
		for _, message := range stream.Messages {
			raw, ok := message.Values["data"]
			if !ok {
				continue
			}

			var bytes []byte
			switch v := raw.(type) {
			case string:
				bytes = []byte(v)
			case []byte:
				bytes = v
			default:
				continue
			}

			var msg Msg
			if err := json.Unmarshal(bytes, &msg); err != nil {
				continue
			}
			msg.StreamID = message.ID
			msg.LastFetchedAt = time.Now()
			result = append(result, msg)

		}
	}

	return result, nil
}

func (ms *MsgStream) CompleteMessage(msg *Msg, success bool, streamName string, groupName string) error {
	if err := ms.client.XAck(ms.ctx, streamName, groupName, msg.StreamID).Err(); err != nil {
		return fmt.Errorf("failed to XACK message %s: %w", msg.StreamID, err)
	}
	if !success {
		msg.RetryCount++

		if msg.RetryCount <= MAX_RETRIES {
			data, err := json.Marshal(msg)
			if err != nil {
				return fmt.Errorf("failed to marshal retry msg: %w", err)
			}

			err = ms.client.XAdd(ms.ctx, &redis.XAddArgs{
				Stream: streamName,
				Values: map[string]interface{}{
					"data": data,
				},
			}).Err()
			if err != nil {
				return fmt.Errorf("failed to requeue message: %w", err)
			}

			return nil
		}

		msg.Status = DEAD
		log.Printf("Message %s is dead!", msg.ID)
		return nil
	}

	msg.Status = DONE
	log.Printf("Message %s successfully processed!", msg.ID)

	return nil

}
