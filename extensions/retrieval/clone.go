package retrieval

func cloneVectorRecords(records []VectorRecord) []VectorRecord {
	cloned := make([]VectorRecord, len(records))
	for index, record := range records {
		cloned[index] = record
		cloned[index].Vector = append([]float32(nil), record.Vector...)
		cloned[index].Metadata = cloneMetadata(record.Metadata)
	}
	return cloned
}

func cloneSearchHits(hits []SearchHit) []SearchHit {
	cloned := make([]SearchHit, len(hits))
	for index, hit := range hits {
		cloned[index] = hit
		cloned[index].Vector = append([]float32(nil), hit.Vector...)
		cloned[index].Metadata = cloneMetadata(hit.Metadata)
		cloned[index].Raw = cloneStringAnyMap(hit.Raw)
	}
	return cloned
}

func cloneStringAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = cloneMetadataValue(value)
	}
	return cloned
}

func cloneMetadata(metadata Metadata) Metadata {
	if metadata == nil {
		return nil
	}
	cloned := make(Metadata, len(metadata))
	for key, value := range metadata {
		cloned[key] = cloneMetadataValue(value)
	}
	return cloned
}

func cloneMetadataValue(value any) any {
	switch current := value.(type) {
	case Metadata:
		return cloneMetadata(current)
	case map[string]any:
		cloned := make(map[string]any, len(current))
		for key, nested := range current {
			cloned[key] = cloneMetadataValue(nested)
		}
		return cloned
	case []any:
		cloned := make([]any, len(current))
		for index := range current {
			cloned[index] = cloneMetadataValue(current[index])
		}
		return cloned
	case []string:
		return append([]string(nil), current...)
	case []float32:
		return append([]float32(nil), current...)
	case []float64:
		return append([]float64(nil), current...)
	case []int:
		return append([]int(nil), current...)
	default:
		return current
	}
}
