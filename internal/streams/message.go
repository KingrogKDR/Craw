package streams

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type MsgStatus string

const (
	READY      = "ready"
	PROCESSING = "processing"
	DONE       = "done"
	FAILED     = "failed"
	DEAD       = "dead"
)

type Msg struct {
	ID            string          `json:"id"`
	StreamID      string          `json:"stream_id"`
	RetryCount    int             `json:"retry_count"`
	Payload       json.RawMessage `json:"payload"`
	Status        MsgStatus       `json:"status"`
	AddedAt       time.Time       `json:"added_at"`
	LastFetchedAt time.Time       `json:"last_fetched_at"`
}

func NewMsg(payload []byte, streamer string) *Msg {
	msgId := fmt.Sprintf("%s-stream-%s", streamer, uuid.New().String())
	return &Msg{
		ID:         msgId,
		RetryCount: 0,
		Payload:    payload,
		Status:     READY,
	}
}
