module app

go 1.20

require (
	github.com/redis/go-redis/v9 v9.0.5
	github.com/skyline93/ctask v0.0.0-00010101000000-000000000000
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/google/uuid v1.3.0 // indirect

)

replace github.com/skyline93/ctask => ../
