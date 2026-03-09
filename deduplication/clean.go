package deduplication

import (
	"bytes"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
)

type SourceType string

const (
	SourceHTML = "html"
	SourceMD   = "md"
)

func CleanData(data string, t SourceType) (string, error) {
	switch t {
	case SourceHTML:
		return cleanHtml(data)
	case SourceMD:
		md := cleanMarkdown(data)
		return md, nil
	}

	return "", nil
}

func cleanHtml(html string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return "", err
	}

	builder := CanonicalBuilder{}

	doc.Find("script, style, nav, footer, aside").Remove()

	title := doc.Find("title").Text()
	builder.Add("title", title)

	doc.Find("h1, h2, h3").Each(func(i int, s *goquery.Selection) {
		builder.Add("heading", s.Text())
	})

	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		builder.Add("text", s.Text())
	})

	doc.Find("li, ul").Each(func(i int, s *goquery.Selection) {
		builder.Add("list", s.Text())
	})

	doc.Find("code").Each(func(i int, s *goquery.Selection) {
		builder.Add("code", s.Text())
	})

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		text := s.Text()
		href, exists := s.Attr("href")
		if exists {
			builder.Add("link", text+" "+href)
		}
	})

	return builder.String(), nil
}

var badgeRegex = regexp.MustCompile(`!\[.*?\]\(.*?shields\.io.*?\)`)
var imageRegex = regexp.MustCompile(`!\[.*?\]\(.*?\)`)

func cleanMarkdown(md string) string {
	md = badgeRegex.ReplaceAllString(md, "") // removes badges from md
	md = imageRegex.ReplaceAllString(md, "") // removes images from md

	mdParser := goldmark.DefaultParser()

	source := []byte(md)
	doc := mdParser.Parse(text.NewReader(source))

	builder := CanonicalBuilder{}

	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}

		switch node := n.(type) {

		case *ast.Heading:
			var buf bytes.Buffer

			for c := node.FirstChild(); c != nil; c = c.NextSibling() {
				if textNode, ok := c.(*ast.Text); ok {
					buf.Write(textNode.Segment.Value(source))
				}
			}

			builder.Add("heading", strings.TrimSpace(buf.String()))

		case *ast.Paragraph:
			var buf bytes.Buffer

			for c := node.FirstChild(); c != nil; c = c.NextSibling() {
				if textNode, ok := c.(*ast.Text); ok {
					buf.Write(textNode.Segment.Value(source))
				}
			}

			builder.Add("text", strings.TrimSpace(buf.String()))

		case *ast.ListItem:
			var buf bytes.Buffer

			for c := node.FirstChild(); c != nil; c = c.NextSibling() {
				if textNode, ok := c.(*ast.Text); ok {
					buf.Write(textNode.Segment.Value(source))
				}
			}

			builder.Add("list", strings.TrimSpace(buf.String()))

		case *ast.Link:
			dest := string(node.Destination)

			var buf bytes.Buffer
			for c := node.FirstChild(); c != nil; c = c.NextSibling() {
				if textNode, ok := c.(*ast.Text); ok {
					buf.Write(textNode.Segment.Value(source))
				}
			}

			text := strings.TrimSpace(buf.String())

			if text != "" {
				builder.Add("link", text+" "+dest)
			} else {
				builder.Add("link", dest)
			}
		}

		return ast.WalkContinue, nil
	})

	return builder.String()
}
