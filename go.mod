module gerrit.o-ran-sc.org/r/ric-plt/sdlgo

go 1.12

require (
	github.com/go-redis/redis/v8 v8.11.4
	github.com/spf13/cobra v1.1.1
	github.com/stretchr/testify v1.5.1
)

replace gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis => ./internal/sdlgoredis
