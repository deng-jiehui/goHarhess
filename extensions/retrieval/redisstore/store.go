// Package redisstore 用带 Search 模块的 Redis（Redis Stack / Redis 8）
// 同时实现向量存储与关键词检索，与会话数据共用一套 Redis。
// 租户隔离是强制的：每个查询都带 @ns 前置过滤，空命名空间直接报错。
package redisstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/goharness/goharness/extensions/retrieval"
	"github.com/goharness/goharness/pkg/runnable"
)

// Distance 是向量距离度量。
type Distance string

const (
	Cosine       Distance = "COSINE"
	InnerProduct Distance = "IP"
	L2           Distance = "L2"
)

// FieldType 是元数据可过滤字段类型。
type FieldType string

const (
	FieldTag     FieldType = "TAG"
	FieldNumeric FieldType = "NUMERIC"
)

// MetadataField 声明一个可过滤的元数据字段。
type MetadataField struct {
	Name string
	Type FieldType
}

// HNSWParams 是 HNSW 索引参数；零值使用 Redis 默认。
type HNSWParams struct {
	M              int
	EFConstruction int
	EFRuntime      int
}

// Config 是 Redis Search 适配器配置。
type Config struct {
	Client   redis.UniversalClient
	Index    string // 索引名
	Prefix   string // 文档 key 前缀
	Dim      int
	Distance Distance
	HNSW     HNSWParams
	Metadata []MetadataField
}

// Store 是适配器本体；通过 VectorStore/KeywordSearch 暴露 Runnable。
type Store struct {
	client   redis.UniversalClient
	index    string
	prefix   string
	dim      int
	distance Distance
	hnsw     HNSWParams
	metadata map[string]MetadataField
	ordered  []MetadataField // 声明序，用于稳定的 schema 构建
}

// New 校验配置。
func New(cfg Config) (*Store, error) {
	if cfg.Client == nil {
		return nil, errors.New("redisstore: client is required")
	}
	if strings.TrimSpace(cfg.Index) == "" {
		return nil, errors.New("redisstore: index name is required")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = cfg.Index + ":"
	}
	if cfg.Dim <= 0 {
		return nil, errors.New("redisstore: Dim must be positive")
	}
	switch cfg.Distance {
	case "":
		cfg.Distance = Cosine
	case Cosine, InnerProduct, L2:
	default:
		return nil, fmt.Errorf("redisstore: unknown distance %q", cfg.Distance)
	}
	metadata := make(map[string]MetadataField, len(cfg.Metadata))
	for _, f := range cfg.Metadata {
		if f.Name == "" || f.Name == "ns" || f.Name == "text" || f.Name == "vec" {
			return nil, fmt.Errorf("redisstore: invalid metadata field name %q", f.Name)
		}
		if f.Type != FieldTag && f.Type != FieldNumeric {
			return nil, fmt.Errorf("redisstore: metadata field %q has unknown type %q", f.Name, f.Type)
		}
		metadata[f.Name] = f
	}
	return &Store{
		client:   cfg.Client,
		index:    cfg.Index,
		prefix:   cfg.Prefix,
		dim:      cfg.Dim,
		distance: cfg.Distance,
		hnsw:     cfg.HNSW,
		metadata: metadata,
		ordered:  cfg.Metadata,
	}, nil
}

// EnsureIndex 幂等建索引；索引已存在时校验维度与距离一致。
func (s *Store) EnsureIndex(ctx context.Context) error {
	raw, err := s.client.Do(ctx, "FT.INFO", s.index).Result()
	if err == nil {
		return s.checkIndex(raw)
	}
	if !isMissingIndex(err) {
		return fmt.Errorf("redisstore: FT.INFO: %w", err)
	}
	return s.createIndex(ctx)
}

func isMissingIndex(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Unknown Index name") ||
		strings.Contains(msg, "no such index") ||
		strings.Contains(msg, "Unknown index name")
}

func (s *Store) createIndex(ctx context.Context) error {
	schema := []*redis.FieldSchema{
		{FieldName: "ns", FieldType: redis.SearchFieldTypeTag},
		{FieldName: "text", FieldType: redis.SearchFieldTypeText},
		{FieldName: "vec", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{
			HNSWOptions: &redis.FTHNSWOptions{
				Type:                   "FLOAT32",
				Dim:                    s.dim,
				DistanceMetric:         string(s.distance),
				MaxEdgesPerNode:        s.hnsw.M,
				EFRunTime:              s.hnsw.EFRuntime,
				MaxAllowedEdgesPerNode: s.hnsw.EFConstruction,
			},
		}},
	}
	for _, f := range s.ordered {
		t := redis.SearchFieldTypeTag
		if f.Type == FieldNumeric {
			t = redis.SearchFieldTypeNumeric
		}
		schema = append(schema, &redis.FieldSchema{FieldName: f.Name, FieldType: t})
	}
	schema = append(schema, &redis.FieldSchema{FieldName: "meta", FieldType: redis.SearchFieldTypeText, NoIndex: true})
	err := s.client.FTCreate(ctx, s.index, &redis.FTCreateOptions{
		OnHash: true,
		Prefix: []interface{}{s.prefix},
	}, schema...).Err()
	if err != nil && isMissingIndex(err) {
		// 并发创建竞态：另一个实例已建好，校验即可。
		return s.EnsureIndex(ctx)
	}
	return err
}

// checkIndex 校验已存在索引的向量维度与距离度量一致。
// go-redis 的 FTInfoResult 不解析向量参数，直接走原始应答；
// RESP2 返回扁平数组，RESP3 返回 map，两种形态都接受。
func (s *Store) checkIndex(raw interface{}) error {
	var attrs []interface{}
	switch v := raw.(type) {
	case []interface{}:
		for i := 0; i+1 < len(v); i += 2 {
			if k, _ := v[i].(string); k == "attributes" {
				attrs, _ = v[i+1].([]interface{})
			}
		}
	case map[interface{}]interface{}:
		for k, val := range v {
			if ks, _ := k.(string); ks == "attributes" {
				attrs, _ = val.([]interface{})
			}
		}
	default:
		return fmt.Errorf("redisstore: unexpected FT.INFO reply type %T", raw)
	}
	for _, item := range attrs {
		fields := map[string]string{}
		switch entry := item.(type) {
		case []interface{}:
			for j := 0; j+1 < len(entry); j += 2 {
				k, _ := entry[j].(string)
				switch v := entry[j+1].(type) {
				case string:
					fields[k] = v
				case int64:
					fields[k] = fmt.Sprint(v)
				}
			}
		case map[interface{}]interface{}:
			for k, v := range entry {
				ks, _ := k.(string)
				switch vv := v.(type) {
				case string:
					fields[ks] = vv
				case int64:
					fields[ks] = fmt.Sprint(vv)
				}
			}
		default:
			continue
		}
		if fields["identifier"] != "vec" {
			continue
		}
		if fields["dim"] != fmt.Sprint(s.dim) {
			return fmt.Errorf("redisstore: index %q dim %s != %d", s.index, fields["dim"], s.dim)
		}
		if !strings.EqualFold(fields["distance_metric"], string(s.distance)) {
			return fmt.Errorf("redisstore: index %q distance %q != %q", s.index, fields["distance_metric"], s.distance)
		}
		return nil
	}
	return fmt.Errorf("redisstore: index %q has no vec field", s.index)
}

// docKey 生成文档 key：<prefix><ns>/<id>。
func (s *Store) docKey(namespace, id string) string {
	return s.prefix + namespace + "/" + id
}

// encodeVector 转 float32 为 little-endian 二进制（RediSearch 参数格式）。
func encodeVector(v []float32) []byte {
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

// hashFields 组装文档 hash：ns/text/vec + 声明字段 + 其余元数据 JSON。
func (s *Store) hashFields(rec retrieval.VectorRecord) (map[string]any, error) {
	if rec.Namespace == "" {
		return nil, errEmptyNamespace
	}
	if len(rec.Vector) != s.dim {
		return nil, fmt.Errorf("redisstore: record %q has %d dims, want %d", rec.ID, len(rec.Vector), s.dim)
	}
	fields := map[string]any{
		"ns":   rec.Namespace,
		"text": rec.Text,
		"vec":  encodeVector(rec.Vector),
	}
	rest := make(map[string]any, len(rec.Metadata))
	for k, v := range rec.Metadata {
		if _, declared := s.metadata[k]; declared {
			fields[k] = v
			continue
		}
		rest[k] = v
	}
	if len(rest) > 0 {
		data, err := json.Marshal(rest)
		if err != nil {
			return nil, fmt.Errorf("redisstore: marshal metadata: %w", err)
		}
		fields["meta"] = string(data)
	}
	return fields, nil
}

// VectorStore 返回向量操作 Runnable。
func (s *Store) VectorStore() runnable.Runnable[retrieval.VectorRequest, retrieval.VectorResponse] {
	return runnable.Func[retrieval.VectorRequest, retrieval.VectorResponse](s.invokeVector)
}

// KeywordSearch 返回关键词检索 Runnable。
func (s *Store) KeywordSearch() runnable.Runnable[retrieval.TextSearchRequest, []retrieval.SearchHit] {
	return runnable.Func[retrieval.TextSearchRequest, []retrieval.SearchHit](s.invokeKeyword)
}

func (s *Store) invokeVector(ctx context.Context, req retrieval.VectorRequest, _ ...runnable.Option) (retrieval.VectorResponse, error) {
	switch req.Operation {
	case retrieval.VectorUpsertOperation:
		return retrieval.VectorResponse{}, s.upsert(ctx, req.Records)
	case retrieval.VectorQueryOperation:
		hits, err := s.vectorQuery(ctx, req.Query)
		return retrieval.VectorResponse{Hits: hits}, err
	case retrieval.VectorDeleteOperation:
		return retrieval.VectorResponse{}, s.delete(ctx, req.Namespace, req.IDs)
	default:
		return retrieval.VectorResponse{}, fmt.Errorf("redisstore: unknown operation %q", req.Operation)
	}
}

func (s *Store) upsert(ctx context.Context, records []retrieval.VectorRecord) error {
	pipe := s.client.Pipeline()
	for _, rec := range records {
		fields, err := s.hashFields(rec)
		if err != nil {
			return err
		}
		pipe.HSet(ctx, s.docKey(rec.Namespace, rec.ID), fields)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) vectorQuery(ctx context.Context, q *retrieval.VectorQuery) ([]retrieval.SearchHit, error) {
	query, k, err := vectorQuery(s, q)
	if err != nil {
		return nil, err
	}
	res, err := s.client.FTSearchWithArgs(ctx, s.index, query, &redis.FTSearchOptions{
		Params:         map[string]interface{}{"vec": encodeVector(q.Vector)},
		Limit:          k,
		Return:         []redis.FTSearchReturn{{FieldName: "text"}, {FieldName: "meta"}},
		DialectVersion: 2,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redisstore: FT.SEARCH: %w", err)
	}
	hits := make([]retrieval.SearchHit, 0, len(res.Docs))
	for _, doc := range res.Docs {
		hit := s.docToHit(doc)
		// score 字段是向量距离；cosine 距离越小越相似。
		if q.MinScore > 0 && (1-hit.Score) < q.MinScore {
			continue
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

func (s *Store) invokeKeyword(ctx context.Context, req retrieval.TextSearchRequest, _ ...runnable.Option) ([]retrieval.SearchHit, error) {
	query, err := keywordQuery(s, req)
	if err != nil {
		return nil, err
	}
	k := req.TopK
	if k <= 0 {
		k = 10
	}
	res, err := s.client.FTSearchWithArgs(ctx, s.index, query, &redis.FTSearchOptions{
		WithScores:     true,
		Limit:          k,
		Return:         []redis.FTSearchReturn{{FieldName: "text"}, {FieldName: "meta"}},
		DialectVersion: 2,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redisstore: FT.SEARCH: %w", err)
	}
	hits := make([]retrieval.SearchHit, 0, len(res.Docs))
	for _, doc := range res.Docs {
		hits = append(hits, s.docToHit(doc))
	}
	return hits, nil
}

// docToHit 把 Redis 文档转成 SearchHit；分数取 score 字段或 WITHSCORES。
func (s *Store) docToHit(doc redis.Document) retrieval.SearchHit {
	hit := retrieval.SearchHit{ID: doc.ID, Source: "redis"}
	if doc.Score != nil {
		hit.Score = *doc.Score
	}
	if v, ok := doc.Fields["score"]; ok {
		// KNN 的 AS score 字段是字符串距离值。
		var f float64
		if _, err := fmt.Sscanf(v, "%g", &f); err == nil {
			hit.Score = f
		}
	}
	hit.Text = doc.Fields["text"]
	if raw := doc.Fields["meta"]; raw != "" {
		meta := retrieval.Metadata{}
		if err := json.Unmarshal([]byte(raw), &meta); err == nil {
			hit.Metadata = meta
		}
	}
	// 声明字段回填到元数据（Tag/Numeric 原样字符串）。
	for name := range s.metadata {
		if v, ok := doc.Fields[name]; ok {
			if hit.Metadata == nil {
				hit.Metadata = retrieval.Metadata{}
			}
			hit.Metadata[name] = v
		}
	}
	// key 里的 ns/id 还原为不带前缀的 ID。
	if rest, ok := strings.CutPrefix(doc.ID, s.prefix); ok {
		if idx := strings.Index(rest, "/"); idx >= 0 {
			hit.ID = rest[idx+1:]
			if hit.Metadata == nil {
				hit.Metadata = retrieval.Metadata{}
			}
			hit.Metadata["_ns"] = rest[:idx]
		}
	}
	return hit
}

// delete 支持按 ID 删除与整命名空间删除；后者分页 FT.SEARCH 取 ID 后
// 批量 UNLINK，不用 KEYS。
func (s *Store) delete(ctx context.Context, namespace string, ids []string) error {
	if len(ids) > 0 {
		keys := make([]string, 0, len(ids))
		for _, id := range ids {
			ns := namespace
			if i := strings.Index(id, "/"); namespace == "" && i >= 0 {
				// 允许 "ns/id" 形态的 ID。
				ns, id = id[:i], id[i+1:]
			}
			if ns == "" {
				return errEmptyNamespace
			}
			keys = append(keys, s.docKey(ns, id))
		}
		return s.client.Unlink(ctx, keys...).Err()
	}
	if namespace == "" {
		return errEmptyNamespace
	}
	// 边删边查：每轮查到的都是仍存活的文档，直到查不到为止。
	query := "@ns:{" + escapeTag(namespace) + "}"
	for {
		res, err := s.client.FTSearchWithArgs(ctx, s.index, query, &redis.FTSearchOptions{
			NoContent:      true,
			LimitOffset:    0,
			Limit:          500,
			DialectVersion: 2,
		}).Result()
		if err != nil {
			return fmt.Errorf("redisstore: namespace delete scan: %w", err)
		}
		if len(res.Docs) == 0 {
			return nil
		}
		keys := make([]string, 0, len(res.Docs))
		for _, doc := range res.Docs {
			keys = append(keys, doc.ID)
		}
		if err := s.client.Unlink(ctx, keys...).Err(); err != nil {
			return err
		}
	}
}

// declaredFieldNames 返回可过滤字段名（调试用）。
func (s *Store) declaredFieldNames() []string {
	out := make([]string, 0, len(s.metadata))
	for k := range s.metadata {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
