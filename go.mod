module github.com/little-cui/etcdadpt

go 1.16

replace (
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)

require (
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // v4
	github.com/go-chassis/foundation v0.3.1-0.20210806081520-3bd92d1ef787
	github.com/go-chassis/go-chassis/v2 v2.2.1-0.20210630123055-6b4c31c5ad02
	github.com/go-chassis/openlog v1.1.3
	github.com/prometheus/client_golang v1.8.0
	github.com/stretchr/testify v1.7.0
)
