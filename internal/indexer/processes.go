package indexer

import (
	"context"

	"github.com/KingrogKDR/Dev-Search/internal/storage"
	"github.com/KingrogKDR/Dev-Search/internal/streams"
)

func CreateAndStoreIndexedDocument(ctx context.Context, msg *streams.Msg, store *storage.MinioStore) {
	// unmarshal the record
	// get the object key
	// text fetched from s3
	// build the index
	// save the document.
}
