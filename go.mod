module gerrit.o-ran-sc.org/r/ric-plt/sdlgo

go 1.12

require (
	github.com/go-redis/redis v6.15.3+incompatible
	github.com/stretchr/testify v1.3.0
	golang.org/x/sys v0.0.0-20190204203706-41f3e6584952 // indirect
)

replace gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis => ./internal/sdlgoredis
