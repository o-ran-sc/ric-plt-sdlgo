module gerrit.o-ran-sc.org/r/ric-plt/sdlgo

go 1.12

require (
	github.com/go-redis/redis/v7 v7.4.1
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/spf13/cobra v1.1.1
	github.com/stretchr/testify v1.3.0
)

replace gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis => ./internal/sdlgoredis
