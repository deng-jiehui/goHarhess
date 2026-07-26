package agent

const DefaultMaxLLMCalls = 20

type RunOptions struct {
	Lang        string
	Seed        *int64
	MaxLLMCalls int
	Knowledge   string
	Metadata    map[string]any
}

type RunOption func(*RunOptions)

func defaultRunOptions() RunOptions {
	return RunOptions{
		MaxLLMCalls: DefaultMaxLLMCalls,
		Metadata:    map[string]any{},
	}
}

func resolveRunOptions(opts ...RunOption) RunOptions {
	ro := defaultRunOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&ro)
		}
	}
	if ro.MaxLLMCalls <= 0 {
		ro.MaxLLMCalls = DefaultMaxLLMCalls
	}
	if ro.Metadata == nil {
		ro.Metadata = map[string]any{}
	}
	return ro
}

func WithLang(lang string) RunOption {
	return func(opts *RunOptions) {
		opts.Lang = lang
	}
}

func WithSeed(seed int64) RunOption {
	return func(opts *RunOptions) {
		opts.Seed = &seed
	}
}

func WithMaxLLMCalls(max int) RunOption {
	return func(opts *RunOptions) {
		opts.MaxLLMCalls = max
	}
}

func WithKnowledge(knowledge string) RunOption {
	return func(opts *RunOptions) {
		opts.Knowledge = knowledge
	}
}

func WithMetadata(key string, value any) RunOption {
	return func(opts *RunOptions) {
		opts.Metadata[key] = value
	}
}
