module gerrit.o-ran-sc.org/r/ric-plt/sdlgo

go 1.12

require (
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.7.0
)

replace gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis => ./internal/sdlgoredis
