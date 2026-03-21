package indexer

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/KingrogKDR/Dev-Search/internal/storage/db"
	"github.com/reiver/go-porterstemmer"
)

var stopwords = map[string]struct{}{
	"a": {}, "an": {}, "the": {},

	"is": {}, "am": {}, "are": {}, "was": {}, "were": {}, "be": {}, "been": {}, "being": {},

	"have": {}, "has": {}, "had": {}, "having": {},

	"do": {}, "does": {}, "did": {}, "doing": {},

	"and": {}, "or": {}, "but": {}, "if": {}, "because": {}, "as": {},

	"until": {}, "while": {}, "i": {}, "we": {}, "me": {}, "myself": {}, "our": {}, "ours": {},

	"of": {}, "at": {}, "by": {}, "for": {}, "with": {}, "about": {}, "against": {},

	"between": {}, "into": {}, "through": {}, "during": {}, "before": {}, "after": {},

	"above": {}, "below": {}, "to": {}, "from": {}, "up": {}, "down": {},

	"in": {}, "out": {}, "on": {}, "off": {}, "over": {}, "under": {},

	"again": {}, "further": {}, "then": {}, "once": {}, "yourself": {}, "ourselves": {},

	"here": {}, "there": {}, "when": {}, "where": {}, "why": {}, "how": {},

	"all": {}, "any": {}, "both": {}, "each": {}, "few": {}, "more": {}, "most": {},

	"other": {}, "some": {}, "such": {}, "you": {}, "yours": {}, "he": {}, "she": {}, "it": {},

	"only": {}, "own": {}, "same": {}, "so": {}, "than": {}, "too": {}, "very": {},

	"can": {}, "will": {}, "just": {}, "should": {}, "now": {},
}

type Document struct {
	Record        *Record
	InvertedIndex map[string]struct{}
}

func NewDocument(record *Record) *Document {
	return &Document{
		Record:        record,
		InvertedIndex: make(map[string]struct{}, 1000),
	}
}

func (d *Document) BuildIndex(text string, recordId uint64) {
	words := strings.Fields(text)

	for _, word := range words {
		word = strings.ToLower(word)

		if _, exists := stopwords[word]; exists {
			continue
		}

		word = stemmer(word)

		d.InvertedIndex[word] = struct{}{}
	}
}

func (d *Document) Save(ctx context.Context) error {
	return insertInvertedIndex(ctx, d)
}

func stemmer(word string) string {
	cleanedWord := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return r
		}
		return -1
	}, word)

	stemmedWord := porterstemmer.StemString(cleanedWord)
	return stemmedWord
}

func insertInvertedIndex(ctx context.Context, doc *Document) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var (
		values []string
		args   []interface{}
		i      = 1
	)

	for term := range doc.InvertedIndex {
		values = append(values, fmt.Sprintf("($%d, $%d)", i, i+1))
		args = append(args, term, doc.Record.ID)
		i += 2
	}

	if len(values) > 0 {
		query := fmt.Sprintf(`
			INSERT INTO inverted_index (term, content_hash)
			VALUES %s
			ON CONFLICT DO NOTHING
		`, strings.Join(values, ","))

		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return err
		}
	}

	// insert document metadata
	_, err = tx.Exec(ctx, `
	INSERT INTO documents (content_hash, url, title, snippet, object_key, inbound_links)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (content_hash) DO NOTHING
	`,
		doc.Record.ID,
		doc.Record.URL,
		doc.Record.Title,
		doc.Record.Snippet,
		doc.Record.TextObjectKey,
		doc.Record.InboundLinks,
	)

	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
