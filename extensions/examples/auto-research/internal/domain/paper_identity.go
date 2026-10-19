package domain

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var (
	doiPattern   = regexp.MustCompile(`^10\.\d{4,9}/\S+$`)
	arxivPattern = regexp.MustCompile(`^(?:\d{4}\.\d{4,5}|[a-z][a-z-]+/\d{7})(?:v\d+)?$`)
)

// NormalizePaperKey 将 DOI 或 arXiv 引用转换为稳定的项目去重键。
func NormalizePaperKey(input string) (string, error) {
	value := strings.TrimSpace(strings.TrimRight(input, ".,;:)]}"))
	if value == "" {
		return "", fmt.Errorf("%w：论文引用不能为空", ErrInvalid)
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "https://doi.org/") || strings.HasPrefix(lower, "http://doi.org/") {
		value = value[strings.Index(value, "://")+3:]
		value = value[strings.Index(value, "/")+1:]
	} else if strings.HasPrefix(lower, "doi:") {
		value = value[len("doi:"):]
	} else if strings.HasPrefix(lower, "https://arxiv.org/") || strings.HasPrefix(lower, "http://arxiv.org/") {
		u, err := url.Parse(value)
		if err != nil {
			return "", fmt.Errorf("%w：arXiv 引用无效", ErrInvalid)
		}
		value = strings.TrimPrefix(strings.TrimPrefix(u.Path, "/abs/"), "/pdf/")
	} else if strings.HasPrefix(lower, "arxiv:") {
		value = value[len("arxiv:"):]
	}
	value = strings.TrimSpace(strings.TrimSuffix(value, ".pdf"))
	if doiPattern.MatchString(strings.ToLower(value)) {
		return "doi:" + strings.ToLower(value), nil
	}
	if arxivPattern.MatchString(strings.ToLower(value)) {
		base := strings.ToLower(value)
		if index := strings.LastIndex(base, "v"); index > 0 && isDigits(base[index+1:]) {
			base = base[:index]
		}
		return "arxiv:" + base, nil
	}
	return "", fmt.Errorf("%w：只支持 DOI 或 arXiv 引用", ErrInvalid)
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
