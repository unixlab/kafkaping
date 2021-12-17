all: clean fmt build
clean:
	rm -f kafkaping
fmt:
	go fmt conf/main.go
	go fmt copy/main.go
	go fmt ping/main.go
	go fmt main.go
build:
	go build
build-docker:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-w -s" -o kafkaping
	docker build .
