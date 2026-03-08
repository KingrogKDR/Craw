package crawler

import (
	"net/http"
	"time"
)

var CrawlerClient = &http.Client{
	Timeout: 10 * time.Second,
}
