package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KingrogKDR/Dev-Search/internal/storage"
	"github.com/KingrogKDR/Dev-Search/internal/streams"
)

func CreateAndStoreIndexedDocument(ctx context.Context, msg *streams.Msg, store *storage.MinioStore) error {
	var record Record
	if err := json.Unmarshal(msg.Payload, &record); err != nil {
		return fmt.Errorf("Can't unmarshal record: %w", err)
	}
	data, err := store.GetObject(ctx, record.TextObjectKey)
	if err != nil {
		return fmt.Errorf("Can't get data from minio: %w", err)
	}
	doc := NewDocument(&record)

	doc.BuildIndex(string(data), record.ID)

	startDB := time.Now()

	if err = doc.Save(ctx); err != nil {
		return fmt.Errorf("Can't save document after indexing: %w", err)
	}

	AddDBLatency(time.Since(startDB))
	return nil
}
