module github.com/goharness/goharness/extensions

go 1.24

require (
	github.com/alicebob/miniredis/v2 v2.35.0
	github.com/goharness/goharness v0.0.0
	github.com/mark3labs/mcp-go v0.48.0
	github.com/redis/go-redis/v9 v9.12.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/spf13/cast v1.7.1 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
)

replace github.com/goharness/goharness => ..
