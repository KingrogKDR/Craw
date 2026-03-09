package deduplication

import "strings"

// canonical format of content extracted for hashing

// title rust ownership tutorial
// heading ownership
// rust uses ownership system to manage memory
// code fn main()
// link rust book https://doc.rust-lang.org/book

type CanonicalBuilder struct {
	lines []string
}

func (b *CanonicalBuilder) Add(prefix, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}

	line := prefix + " " + value
	b.lines = append(b.lines, line)
}

func (b *CanonicalBuilder) String() string {
	return strings.Join(b.lines, "\n")
}
