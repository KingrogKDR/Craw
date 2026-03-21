package indexer

type Record struct {
	ID            uint64 // text object Key
	URL           string
	TextObjectKey string
	Title         string
	Snippet       string
	InboundLinks  int
}

func NewRecord(hash uint64, rawUrl string, title string, snippet string, objectKey string, inboundLinks int) *Record {
	return &Record{
		ID:            hash,
		URL:           rawUrl,
		TextObjectKey: objectKey,
		Title:         title,
		Snippet:       snippet,
		InboundLinks:  inboundLinks,
	}
}
