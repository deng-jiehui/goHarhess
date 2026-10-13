package redisstore

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/goharness/goharness/extensions/retrieval"
)

var errEmptyNamespace = errors.New("redisstore: namespace is required")

// RediSearch 查询语法需要转义的字符集（tag 值与短语引号内同样处理）。
const escapeChars = `,.<>{}[]"':;!@#$%^&*()-+=~| ` + "\\"

// escapeTag 转义 Tag 字段值里会破坏查询语法的字符。
func escapeTag(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 4)
	for _, r := range value {
		if strings.ContainsRune(escapeChars, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// escapePhrase 把一段用户文本转成 RediSearch 安全的形式：双引号包裹、
// 内部引号与反斜杠转义。返回的查询是短语匹配（term 全部命中）。
func escapePhrase(text string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range text {
		if r == '"' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
}

// escapeTerms 逐词转义后按短语 OR 组合：`"w1" "w2"` 与单引号短语相比保留
// 分词语义；任何一词命中即可，配合 BM25 排序。
func escapeTerms(query string) string {
	terms := strings.Fields(query)
	if len(terms) == 0 {
		return ""
	}
	parts := make([]string, len(terms))
	for i, term := range terms {
		parts[i] = escapePhrase(term)
	}
	return strings.Join(parts, " ")
}

// nsFilter 生成强制命名空间前置过滤；ns 为空返回错误。
func nsFilter(namespace string) (string, error) {
	if namespace == "" {
		return "", errEmptyNamespace
	}
	return "@ns:{" + escapeTag(namespace) + "}", nil
}

// metadataFilter 把声明过的元数据过滤转成查询子句；未声明字段报错。
func metadataFilter(field MetadataField, value any) (string, error) {
	switch field.Type {
	case FieldTag:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("metadata filter %q expects a string tag value", field.Name)
		}
		return "@" + field.Name + ":{" + escapeTag(v) + "}", nil
	case FieldNumeric:
		switch v := value.(type) {
		case float64:
			return fmt.Sprintf("@%s:[%v %v]", field.Name, v, v), nil
		case int:
			return fmt.Sprintf("@%s:[%d %d]", field.Name, v, v), nil
		case [2]float64:
			return fmt.Sprintf("@%s:[%v %v]", field.Name, v[0], v[1]), nil
		default:
			return "", fmt.Errorf("metadata filter %q expects a number or [2]float64 range", field.Name)
		}
	default:
		return "", fmt.Errorf("metadata field %q has unknown type %q", field.Name, field.Type)
	}
}

// filtersClause 拼接全部元数据过滤子句；字段名排序保证输出稳定。
func filtersClause(s *Store, filters retrieval.Metadata) (string, error) {
	if len(filters) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(filters))
	for k := range filters {
		if k == nsFilterKey {
			continue // 命名空间键单独处理，不是元数据过滤
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var clauses []string
	for _, k := range keys {
		field, ok := s.metadata[k]
		if !ok {
			return "", fmt.Errorf("metadata filter %q is not a declared field", k)
		}
		clause, err := metadataFilter(field, filters[k])
		if err != nil {
			return "", err
		}
		clauses = append(clauses, clause)
	}
	return strings.Join(clauses, " "), nil
}

// vectorQuery 构造 KNN 查询：`@ns:{..} (过滤)=>[KNN k @vec $vec AS score]`。
func vectorQuery(s *Store, q *retrieval.VectorQuery) (query string, k int, err error) {
	if q == nil {
		return "", 0, fmt.Errorf("vector query is required")
	}
	ns, err := nsFilter(q.Namespace)
	if err != nil {
		return "", 0, err
	}
	filters, err := filtersClause(s, q.Filter)
	if err != nil {
		return "", 0, err
	}
	k = q.TopK
	if k <= 0 {
		k = 10
	}
	base := ns
	if filters != "" {
		base = "(" + ns + " " + filters + ")"
	}
	return base + fmt.Sprintf("=>[KNN %d @vec $vec AS score]", k), k, nil
}

// keywordQuery 构造关键词查询：`@ns:{..} (过滤) "词" "词"`。
func keywordQuery(s *Store, req retrieval.TextSearchRequest) (query string, err error) {
	ns, err := nsFilter(keywordNamespace(req))
	if err != nil {
		return "", err
	}
	filters, err := filtersClause(s, req.Filters)
	if err != nil {
		return "", err
	}
	terms := escapeTerms(req.Query)
	if terms == "" {
		return "", fmt.Errorf("keyword query text is empty")
	}
	parts := []string{ns}
	if filters != "" {
		parts = append(parts, filters)
	}
	parts = append(parts, terms)
	return strings.Join(parts, " "), nil
}

// listQuery 构造纯过滤列举查询：`@ns:{..} (过滤)`，不带向量与关键词，
// 供入库清理等维护路径分页扫描。
func listQuery(s *Store, namespace string, filters retrieval.Metadata) (string, error) {
	ns, err := nsFilter(namespace)
	if err != nil {
		return "", err
	}
	clauses, err := filtersClause(s, filters)
	if err != nil {
		return "", err
	}
	if clauses == "" {
		return ns, nil
	}
	return ns + " " + clauses, nil
}

// nsFilterKey 是关键词查询里携带命名空间的保留键——TextSearchRequest
// 没有独立 Namespace 字段，由 Scoped 调用方以该键注入。
const nsFilterKey = "_ns"

func keywordNamespace(req retrieval.TextSearchRequest) string {
	if req.Filters == nil {
		return ""
	}
	v, _ := req.Filters[nsFilterKey].(string)
	return v
}
