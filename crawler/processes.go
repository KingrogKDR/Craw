package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/KingrogKDR/Dev-Search/storage"
	"github.com/redis/go-redis/v9"
	"github.com/temoto/robotstxt"
	"golang.org/x/sync/singleflight"
)

type DomainMeta struct {
	Host            string        `json:"host"`
	RobotsRaw       []byte        `json:"robots_raw"`
	NextAllowedTime time.Time     `json:"next_allowed_time"`
	CrawlDelay      time.Duration `json:"crawl_delay"`
}

const (
	DomainMetaKey = "domainmeta:%s"
)

var domainMetaGroup singleflight.Group

func CheckDomainRateLimit(meta *DomainMeta) (bool, error) {
	return !time.Now().Before(meta.NextAllowedTime), nil
}

func UpdateDomainAccess(ctx context.Context, domain string, meta *DomainMeta) error {
	meta.NextAllowedTime = time.Now().Add(meta.CrawlDelay)

	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	key := fmt.Sprintf(DomainMetaKey, domain)

	return storage.GetRedisClient().Set(ctx, key, data, 24*time.Hour).Err()
}

func GetDomainMetadata(ctx context.Context, domain string, scheme string) (*DomainMeta, error) {
	rdb := storage.GetRedisClient()
	key := fmt.Sprintf(DomainMetaKey, domain)

	data, err := rdb.Get(ctx, key).Bytes()

	if err == nil {
		var meta DomainMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, err
		}
		return &meta, nil
	}

	if err != redis.Nil {
		return nil, err
	}

	v, err, _ := domainMetaGroup.Do(domain, func() (interface{}, error) {
		robotsUrl := fmt.Sprintf("%s://%s/robots.txt", scheme, domain)

		robotsResp, err := FetchReq(ctx, robotsUrl)

		if err != nil {
			return nil, err
		}

		defer robotsResp.Body.Close()

		body, err := io.ReadAll(robotsResp.Body)
		if err != nil {
			return nil, err
		}

		robots, _ := robotstxt.FromBytes(body)

		delay := time.Second

		if robots != nil {
			group := robots.FindGroup(UserAgent)
			if group.CrawlDelay > 0 {
				delay = group.CrawlDelay
			}
		}

		meta := DomainMeta{
			Host:            domain,
			RobotsRaw:       body,
			CrawlDelay:      delay,
			NextAllowedTime: time.Now(),
		}

		encoded, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}

		if err := rdb.Set(ctx, key, encoded, 24*time.Hour).Err(); err != nil {
			return nil, err
		}

		return &meta, nil
	})

	if err != nil {
		return nil, err
	}

	return v.(*DomainMeta), nil
}

func IsAllowedByRobots(ctx context.Context, meta *DomainMeta, rawUrl string) (bool, error) {
	parsed, err := url.Parse(rawUrl)
	if err != nil {
		return false, fmt.Errorf("invalid url: %w", err)
	}

	if len(meta.RobotsRaw) == 0 {
		return true, nil
	}

	robots, err := robotstxt.FromBytes(meta.RobotsRaw)
	if err != nil || robots == nil {
		return true, nil
	}

	group := robots.FindGroup(UserAgent)

	return group.Test(parsed.Path), nil
}

func FetchReq(ctx context.Context, rawUrl string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", rawUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request for %s: %w", rawUrl, err)
	}
	request.Header.Set("User-Agent", UserAgent)

	resp, err := CrawlerClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Error fetching request for %s: %w", rawUrl, err)
	}

	return resp, nil
}
